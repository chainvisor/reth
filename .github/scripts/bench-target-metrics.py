#!/usr/bin/env python3
"""Scrape configured Prometheus target metrics for bench comparisons.

This intentionally supports only the small query subset used by the bench
config today:
  - metric_name
  - metric_name{label="value"}
  - sum(metric_name{label="value"})
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request

EPSILON = 1e-12
SAMPLE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+"
    r"(?P<value>[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|[+-]?Inf|NaN)"
    r"(?:\s+[0-9]+)?$"
)
SELECTOR_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?$"
)


def parse_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def load_metrics(url: str) -> str:
    with urllib.request.urlopen(url) as resp:
        return resp.read().decode("utf-8")


def parse_label_string(text: str | None) -> dict[str, str]:
    if not text:
        return {}

    labels = {}
    parts = []
    current = []
    in_quotes = False
    escaped = False
    for ch in text:
        if escaped:
            current.append(ch)
            escaped = False
            continue
        if ch == "\\":
            current.append(ch)
            escaped = True
            continue
        if ch == '"':
            current.append(ch)
            in_quotes = not in_quotes
            continue
        if ch == "," and not in_quotes:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(ch)
    if current:
        parts.append("".join(current).strip())

    for part in parts:
        if not part:
            continue
        key, value = part.split("=", 1)
        labels[key.strip()] = bytes(value.strip()[1:-1], "utf-8").decode("unicode_escape")
    return labels


def parse_samples(text: str) -> list[dict]:
    samples = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = SAMPLE_RE.match(line)
        if not match:
            continue
        samples.append(
            {
                "name": match.group("name"),
                "labels": parse_label_string(match.group("labels")),
                "value": float(match.group("value")),
            }
        )
    return samples


def parse_query(query: str) -> tuple[str, str, dict[str, str]]:
    query = query.strip()
    aggregate = "single"
    inner = query
    if query.startswith("sum(") and query.endswith(")"):
        aggregate = "sum"
        inner = query[4:-1].strip()

    match = SELECTOR_RE.match(inner)
    if not match:
        raise ValueError(f"Unsupported target metric query: {query}")
    return aggregate, match.group("name"), parse_label_string(match.group("labels"))


def evaluate_query(samples: list[dict], query: str) -> float:
    aggregate, metric_name, label_filters = parse_query(query)
    matches = [
        sample["value"]
        for sample in samples
        if sample["name"] == metric_name
        and all(sample["labels"].get(key) == value for key, value in label_filters.items())
    ]

    if not matches:
        raise ValueError(f"Query matched no samples: {query}")

    if aggregate == "sum":
        return float(sum(matches))
    if len(matches) > 1:
        raise ValueError(
            f"Query matched {len(matches)} samples; use sum(...) or label filters: {query}"
        )
    return float(matches[0])


def scrape_values(config_path: str, metrics_url: str) -> dict:
    config = parse_config(config_path)
    samples = parse_samples(load_metrics(metrics_url))
    counters = []
    for counter in config.get("counters", []):
        query = counter["query"]
        counters.append(
            {
                "query": query,
                "target": counter["target"],
                "value": evaluate_query(samples, query),
            }
        )
    return {"counters": counters}


def diff_values(start_path: str, end_path: str) -> dict:
    with open(start_path) as f:
        start = json.load(f)
    with open(end_path) as f:
        end = json.load(f)

    start_by_query = {item["query"]: item for item in start.get("counters", [])}
    end_by_query = {item["query"]: item for item in end.get("counters", [])}

    counters = []
    for query, start_item in start_by_query.items():
        if query not in end_by_query:
            raise ValueError(f"Missing end metric for query: {query}")
        end_item = end_by_query[query]
        counters.append(
            {
                "query": query,
                "target": start_item["target"],
                "start_value": float(start_item["value"]),
                "end_value": float(end_item["value"]),
                "delta": float(end_item["value"] - start_item["value"]),
            }
        )
    return {"counters": counters}


def write_json(path: str, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape target Prometheus metrics for benches")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scrape = subparsers.add_parser("scrape", help="Scrape configured metrics from a Prometheus endpoint")
    scrape.add_argument("--metrics-url", required=True, help="Metrics endpoint URL")
    scrape.add_argument("--config", required=True, help="Target metrics config path")
    scrape.add_argument("--output", required=True, help="Output JSON path")

    delta = subparsers.add_parser("diff", help="Compute deltas between two metric snapshots")
    delta.add_argument("--start", required=True, help="Start snapshot JSON path")
    delta.add_argument("--end", required=True, help="End snapshot JSON path")
    delta.add_argument("--output", required=True, help="Output JSON path")

    args = parser.parse_args()

    try:
        if args.command == "scrape":
            write_json(args.output, scrape_values(args.config, args.metrics_url))
        else:
            write_json(args.output, diff_values(args.start, args.end))
    except Exception as err:  # pragma: no cover - CLI error path
        print(str(err), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
