//! Prometheus metrics scraper for reth-bench.
//!
//! Scrapes a node's Prometheus metrics endpoint after each block.

use eyre::Context;
use reqwest::Client;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{fs::File, io::AsyncWriteExt, sync::mpsc, task::JoinHandle};
use tracing::info;

/// Suffix for the metrics JSONL output file.
pub(crate) const METRICS_OUTPUT_SUFFIX: &str = "metrics.jsonl";

/// A single scraped Prometheus metric sample.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct MetricsSample {
    /// Metric name.
    pub(crate) name: String,
    /// Label key-value pairs. Ordered for deterministic serialization.
    pub(crate) labels: BTreeMap<String, String>,
    /// Metric value.
    pub(crate) value: f64,
    /// Monotonic offset in milliseconds since scraping started.
    pub(crate) offset_ms: u64,
    /// Wall-clock time in Unix milliseconds.
    pub(crate) unix_ms: u64,
}

/// Raw scrape text and timestamps queued for background parsing and writing.
#[derive(Debug, Clone)]
pub(crate) struct MetricsScrape {
    /// Raw Prometheus exposition-format metrics text.
    pub(crate) text: String,
    /// Monotonic offset in milliseconds since scraping started.
    pub(crate) offset_ms: u64,
    /// Wall-clock time in Unix milliseconds.
    pub(crate) unix_ms: u64,
}

/// Scrapes a Prometheus metrics endpoint after each block and writes JSONL records.
pub(crate) struct MetricsScraper {
    /// The full URL of the Prometheus metrics endpoint.
    url: String,
    /// Reusable HTTP client.
    client: Client,
    /// Sends raw scrapes to the background parser/writer task.
    sender: mpsc::Sender<MetricsScrape>,
    /// Monotonic start time for computing sample offsets.
    started_at: Instant,
    /// Background writer task.
    writer_task: JoinHandle<eyre::Result<()>>,
}

impl MetricsScraper {
    /// Creates a new scraper if a URL is provided.
    pub(crate) fn maybe_new(
        url: Option<String>,
        output_path: Option<PathBuf>,
    ) -> eyre::Result<Option<Self>> {
        match (url, output_path) {
            (Some(url), Some(output_path)) => {
                info!(target: "reth-bench", %url, path = %output_path.display(), "Prometheus metrics scraping enabled");
                let client = Client::builder()
                    .timeout(Duration::from_secs(5))
                    .build()
                    .expect("failed to build reqwest client");
                let (sender, mut receiver) = mpsc::channel::<MetricsScrape>(1024);
                let writer_task = tokio::spawn(async move {
                    let mut file = File::create(&output_path).await.wrap_err_with(|| {
                        format!("failed to create metrics output file {}", output_path.display())
                    })?;
                    while let Some(scrape) = receiver.recv().await {
                        for sample in parse_samples(&scrape.text, scrape.offset_ms, scrape.unix_ms)
                        {
                            let line = serde_json::to_string(&sample)
                                .wrap_err("failed to serialize metrics sample")?;
                            file.write_all(line.as_bytes()).await?;
                            file.write_all(b"\n").await?;
                        }
                    }
                    file.flush().await?;
                    Ok(())
                });

                Ok(Some(Self { url, client, sender, started_at: Instant::now(), writer_task }))
            }
            (Some(_), None) => eyre::bail!(
                "--metrics-url requires --metrics-output or --output to choose a JSONL output path"
            ),
            (None, _) => Ok(None),
        }
    }

    /// Scrapes the metrics endpoint and queues samples for writing.
    pub(crate) async fn scrape_after_block(&self) -> eyre::Result<()> {
        let text = self
            .client
            .get(&self.url)
            .send()
            .await
            .wrap_err("failed to fetch metrics endpoint")?
            .error_for_status()
            .wrap_err("metrics endpoint returned error status")?
            .text()
            .await
            .wrap_err("failed to read metrics response body")?;

        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .wrap_err("system time is before unix epoch")?
            .as_millis() as u64;
        let offset_ms = self.started_at.elapsed().as_millis() as u64;

        let scrape = MetricsScrape { text, offset_ms, unix_ms };
        self.sender.send(scrape).await.wrap_err("failed to queue metrics scrape for writing")?;

        Ok(())
    }

    /// Waits for the writer task to finish.
    pub(crate) async fn finish(self) -> eyre::Result<()> {
        let Self { sender, writer_task, .. } = self;

        drop(sender);
        writer_task.await.wrap_err("metrics writer task failed")??;
        Ok(())
    }
}

fn parse_samples(text: &str, offset_ms: u64, unix_ms: u64) -> Vec<MetricsSample> {
    text.lines().filter_map(|line| parse_sample_line(line, offset_ms, unix_ms)).collect()
}

fn parse_sample_line(line: &str, offset_ms: u64, unix_ms: u64) -> Option<MetricsSample> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None
    }

    let (selector, rest) = split_selector_and_value(line)?;
    let value = rest.split_whitespace().next()?.parse::<f64>().ok()?;
    let (name, labels) = parse_selector(selector)?;

    Some(MetricsSample { name, labels, value, offset_ms, unix_ms })
}

fn split_selector_and_value(line: &str) -> Option<(&str, &str)> {
    let mut in_quotes = false;
    let mut escaped = false;
    for (index, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue
        }
        if ch == '\\' {
            escaped = true;
            continue
        }
        if ch == '"' {
            in_quotes = !in_quotes;
            continue
        }
        if ch.is_whitespace() && !in_quotes {
            let rest = line[index..].trim_start();
            if rest.is_empty() {
                return None
            }
            return Some((&line[..index], rest))
        }
    }
    None
}

fn parse_selector(selector: &str) -> Option<(String, BTreeMap<String, String>)> {
    let Some(labels_start) = selector.find('{') else {
        return Some((selector.to_string(), BTreeMap::new()))
    };
    let labels_end = selector.rfind('}')?;
    if labels_end <= labels_start {
        return None
    }

    let name = selector[..labels_start].to_string();
    let labels = parse_labels(&selector[labels_start + 1..labels_end])?;
    Some((name, labels))
}

fn parse_labels(text: &str) -> Option<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    for part in split_label_parts(text) {
        let (key, value) = part.split_once('=')?;
        labels.insert(key.trim().to_string(), parse_label_value(value.trim())?);
    }
    Some(labels)
}

fn split_label_parts(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escaped = false;

    for ch in text.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue
        }
        if ch == '\\' {
            current.push(ch);
            escaped = true;
            continue
        }
        if ch == '"' {
            current.push(ch);
            in_quotes = !in_quotes;
            continue
        }
        if ch == ',' && !in_quotes {
            parts.push(current.trim().to_string());
            current.clear();
            continue
        }
        current.push(ch);
    }

    if !current.is_empty() {
        parts.push(current.trim().to_string());
    }

    parts
}

fn parse_label_value(text: &str) -> Option<String> {
    let quoted = text.strip_prefix('"')?.strip_suffix('"')?;
    let mut value = String::new();
    let mut chars = quoted.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            value.push(ch);
            continue
        }

        match chars.next()? {
            'n' => value.push('\n'),
            '\\' => value.push('\\'),
            '"' => value.push('"'),
            escaped => value.push(escaped),
        }
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_sample_serializes_like_txgen_sample() {
        let sample = MetricsSample {
            name: "example".to_string(),
            labels: BTreeMap::from([("kind".to_string(), "test".to_string())]),
            value: 1.5,
            offset_ms: 42,
            unix_ms: 123,
        };

        let value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&sample).unwrap()).unwrap();
        assert_eq!(value["name"], "example");
        assert_eq!(value["labels"]["kind"], "test");
        assert_eq!(value["value"], 1.5);
        assert_eq!(value["offset_ms"], 42);
        assert_eq!(value["unix_ms"], 123);
    }

    #[test]
    fn test_parse_samples() {
        let samples = parse_samples(
            r#"# HELP example_total example
example_total{kind="a",escaped="quote\" newline\n"} 12.5
other 3 12345
"#,
            42,
            123,
        );

        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].name, "example_total");
        assert_eq!(samples[0].labels["kind"], "a");
        assert_eq!(samples[0].labels["escaped"], "quote\" newline\n");
        assert_eq!(samples[0].value, 12.5);
        assert_eq!(samples[0].offset_ms, 42);
        assert_eq!(samples[0].unix_ms, 123);
        assert_eq!(samples[1].name, "other");
        assert!(samples[1].labels.is_empty());
        assert_eq!(samples[1].value, 3.0);
    }
}
