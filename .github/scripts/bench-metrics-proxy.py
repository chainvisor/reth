#!/usr/bin/env python3
"""
Prometheus metrics proxy that fetches from a local reth node and
re-exposes with additional benchmark labels.

Reads labels from a JSON file (updated by local-reth-bench.sh between runs)
and injects them into every Prometheus metric line.

Returns empty 200 when reth is not running (clean Grafana gaps).
"""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.error import URLError
from urllib.request import ProxyHandler, build_opener


TARGET_METRIC_BLOCK_HEIGHT_QUERY = "reth_blockchain_tree_canonical_chain_height"
HISTOGRAM_QUANTILES = (("p50", "0.5"), ("p90", "0.9"), ("p99", "0.99"))
SAMPLE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+"
    r"(?P<value>[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|[+-]?Inf|NaN)"
    r"(?:\s+[0-9]+)?$"
)
SELECTOR_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?$"
)
INTERNAL_LABEL_KEYS = ("run_start_epoch", "reference_epoch", "target_metrics_file")

# The benchmark runner environment may set HTTP proxy variables. Bypass them for
# local upstream scrapes so the proxy always talks directly to reth's loopback
# metrics endpoint.
DIRECT_URL_OPENER = build_opener(ProxyHandler({}))


def safe_print(message, *, stream=sys.stdout):
    try:
        print(message, file=stream, flush=True)
    except (BrokenPipeError, OSError):
        pass


def configure_ci_process_lifecycle():
    """Keep the proxy alive across GitHub Actions benchmark steps.

    The benchmark workflows launch this proxy in the background in one step, then
    run the actual benchmark in later steps. Put the proxy in its own process
    group and ignore SIGHUP so it does not inherit the shell lifecycle from the
    setup step.
    """
    if os.name != "posix" or os.environ.get("GITHUB_ACTIONS") != "true":
        return

    try:
        os.setsid()
    except OSError:
        try:
            os.setpgrp()
        except OSError:
            pass

    try:
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
    except (AttributeError, OSError, ValueError):
        pass


def read_labels(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def read_target_metrics_config(path):
    with open(path) as f:
        return json.load(f)


def parse_label_string(text):
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


def parse_target_metric_query(query):
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


def format_label_value(value):
    return value.replace("\\", "\\\\").replace('"', '\\"')


def format_target_metric_query(metric_name, labels):
    if not labels:
        return metric_name
    encoded_labels = ",".join(
        f'{key}="{format_label_value(value)}"' for key, value in sorted(labels.items())
    )
    return f"{metric_name}{{{encoded_labels}}}"


def histogram_quantile_query(query, quantile):
    aggregate, metric_name, label_filters = parse_target_metric_query(query)
    if aggregate != "single":
        raise ValueError(f"Histogram target metric queries must not use sum(...): {query}")
    if "quantile" in label_filters:
        raise ValueError(f"Histogram target metric query must not include a quantile label: {query}")
    label_filters = dict(label_filters)
    label_filters["quantile"] = quantile
    return format_target_metric_query(metric_name, label_filters)


def configured_target_metric_queries(config):
    queries = [TARGET_METRIC_BLOCK_HEIGHT_QUERY]
    queries.extend(counter["query"] for counter in config.get("counters", []))
    for histogram in config.get("histograms", []):
        for _, quantile in HISTOGRAM_QUANTILES:
            queries.append(histogram_quantile_query(histogram["query"], quantile))
    return queries


def query_matches_sample(sample, metric_name, label_filters):
    return sample["name"] == metric_name and all(
        sample["labels"].get(key) == value for key, value in label_filters.items()
    )


def query_samples(samples, query):
    aggregate, metric_name, label_filters = parse_target_metric_query(query)
    matches = [
        sample
        for sample in samples
        if query_matches_sample(sample, metric_name, label_filters)
    ]
    return aggregate, matches


def parse_samples(metrics_text):
    samples = []
    for line in metrics_text.splitlines():
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


def evaluate_query(samples, query, allow_missing=False):
    aggregate, matched_samples = query_samples(samples, query)
    matches = [sample["value"] for sample in matched_samples]

    if not matches:
        if allow_missing:
            return 0.0
        raise ValueError(f"Query matched no samples: {query}")

    if aggregate == "sum":
        return float(sum(matches))
    if len(matches) > 1:
        raise ValueError(
            f"Query matched {len(matches)} samples; use sum(...) or label filters: {query}"
        )
    return float(matches[0])


def target_metric_sample_key(sample):
    return sample["name"], tuple(sorted(sample["labels"].items()))


def scrape_target_metrics(metrics_text, config):
    samples = parse_samples(metrics_text)
    queries = configured_target_metric_queries(config)
    target_samples = []
    seen = set()

    for query in queries:
        aggregate, matches = query_samples(samples, query)
        if query == TARGET_METRIC_BLOCK_HEIGHT_QUERY and not matches:
            raise ValueError(f"Query matched no samples: {query}")
        if not matches:
            continue
        if aggregate != "sum" and len(matches) > 1:
            raise ValueError(
                f"Query matched {len(matches)} samples; use sum(...) or label filters: {query}"
            )
        for sample in matches:
            key = target_metric_sample_key(sample)
            if key in seen:
                continue
            seen.add(key)
            target_samples.append(
                {
                    "name": sample["name"],
                    "labels": dict(sorted(sample["labels"].items())),
                    "value": float(sample["value"]),
                }
            )

    return target_samples


def compute_target_metric_offset_ms(labels, unix_ms):
    start = labels.get("run_start_epoch")
    if not start:
        return 0
    try:
        return max(unix_ms - int(float(start) * 1000), 0)
    except (ValueError, TypeError):
        return 0


class TargetMetricScraper(threading.Thread):
    def __init__(self, labels_file, upstream, config_path, interval_s):
        super().__init__(daemon=True)
        self.labels_file = labels_file
        self.upstream = upstream
        self.config = read_target_metrics_config(config_path)
        self.interval_s = interval_s
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            try:
                self.scrape_once()
            except Exception as exc:
                safe_print(f"target metric scrape failed: {exc}", stream=sys.stderr)
            self.stop_event.wait(self.interval_s)

    def scrape_once(self):
        labels = read_labels(self.labels_file)
        output_path = labels.get("target_metrics_file")
        if not output_path:
            return

        try:
            with DIRECT_URL_OPENER.open(self.upstream, timeout=2) as resp:
                metrics_text = resp.read().decode("utf-8")
        except (URLError, ConnectionError, OSError):
            return

        target_samples = scrape_target_metrics(metrics_text, self.config)
        unix_ms = time.time_ns() // 1_000_000
        offset_ms = compute_target_metric_offset_ms(labels, unix_ms)
        records = [
            {
                "name": sample["name"],
                "labels": sample["labels"],
                "value": sample["value"],
                "offset_ms": offset_ms,
                "unix_ms": unix_ms,
            }
            for sample in target_samples
        ]

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "a") as f:
            for record in records:
                json.dump(record, f)
                f.write("\n")


def inject_labels(metrics_bytes, label_str, label_names):
    """Inject labels into Prometheus text format.

    Operates on bytes and uses simple string ops instead of regex
    for speed on large payloads (reth exposes thousands of metrics).

    Skips injecting into lines that already contain any of the label names
    to avoid duplicate labels (which Prometheus rejects).
    """
    if not label_str:
        return metrics_bytes

    label_bytes = label_str.encode("utf-8")
    # Pre-encode label names for fast duplicate detection
    label_name_bytes = [n.encode("utf-8") for n in label_names]
    out = []
    for line in metrics_bytes.split(b"\n"):
        # Skip comments and blank lines
        if line.startswith(b"#") or not line:
            out.append(line)
            continue

        brace = line.find(b"{")
        space = line.find(b" ")

        if space == -1:
            # Malformed, pass through
            out.append(line)
        elif brace != -1 and brace < space:
            # Has labels: metric{existing="val"} 123
            close = line.find(b"}", brace)
            if close == -1:
                out.append(line)
                continue

            # Filter out labels that already exist in this line
            existing = line[brace + 1:close]
            inject = label_bytes
            if existing:
                for name in label_name_bytes:
                    if name + b"=" in existing:
                        # Rebuild inject string excluding this label
                        inject = _remove_label(inject, name)
                if not inject:
                    out.append(line)
                    continue

            if close == brace + 1:
                # Empty braces: metric{} 123
                out.append(line[:close] + inject + line[close:])
            else:
                out.append(line[:close] + b"," + inject + line[close:])
        else:
            # No labels: metric 123
            out.append(line[:space] + b"{" + label_bytes + b"}" + line[space:])

    return b"\n".join(out)


def _remove_label(label_bytes, name):
    """Remove a single label (name=\"...\") from a comma-separated label string."""
    parts = []
    for part in label_bytes.split(b","):
        if not part.startswith(name + b"="):
            parts.append(part)
    return b",".join(parts)


def build_label_str(labels):
    """Pre-format the label injection string: key1="val1",key2="val2" """
    if not labels:
        return ""
    return ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))


def build_elapsed_gauge(labels):
    """Build a bench_elapsed_seconds gauge from run_start_epoch in labels."""
    start = labels.get("run_start_epoch")
    if not start:
        return b""
    try:
        elapsed = time.time() - float(start)
    except (ValueError, TypeError):
        return b""
    # Build labels excluding internal keys
    display = {k: v for k, v in labels.items() if k not in INTERNAL_LABEL_KEYS}
    lstr = build_label_str(display)
    return (
        f"# HELP bench_elapsed_seconds Seconds since benchmark run started\n"
        f"# TYPE bench_elapsed_seconds gauge\n"
        f"bench_elapsed_seconds{{{lstr}}} {elapsed:.1f}\n"
    ).encode("utf-8")


def compute_timestamp_ms(labels):
    """Compute a synthetic timestamp so all runs share a common time origin.

    Returns the timestamp in milliseconds, or None if not enough info.
    Uses: reference_epoch + (now - run_start_epoch) → all runs overlay at
    the same Grafana time range.
    """
    ref = labels.get("reference_epoch")
    start = labels.get("run_start_epoch")
    if not ref or not start:
        return None
    try:
        elapsed = time.time() - float(start)
        return int((float(ref) + elapsed) * 1000)
    except (ValueError, TypeError):
        return None


def inject_timestamps(metrics_bytes, timestamp_ms):
    """Append a Prometheus timestamp (ms) to every data line.

    Prometheus text format: metric{labels} value [timestamp_ms]
    Adding timestamps causes Prometheus to store all runs' samples
    at the same relative time, enabling natural overlay in Grafana.
    """
    if timestamp_ms is None:
        return metrics_bytes

    ts = str(timestamp_ms).encode("utf-8")
    out = []
    for line in metrics_bytes.split(b"\n"):
        if line.startswith(b"#") or not line:
            out.append(line)
        else:
            out.append(line + b" " + ts)
    return b"\n".join(out)


class MetricsHandler(BaseHTTPRequestHandler):
    # Use HTTP/1.1 so Content-Length is respected and Prometheus
    # doesn't have to rely on connection close to detect end of body.
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        src = self.client_address[0]
        try:
            resp = DIRECT_URL_OPENER.open(self.server.upstream, timeout=2)
            metrics = resp.read()
        except (URLError, ConnectionError, OSError):
            # reth not running — return empty 200
            self._send(b"")
            #print(f"  scrape from {src}: empty (reth not running)", flush=True)
            return

        all_labels = read_labels(self.server.labels_file)
        # Internal keys — not injected as Prometheus labels
        labels = {k: v for k, v in all_labels.items() if k not in INTERNAL_LABEL_KEYS}
        label_str = build_label_str(labels)
        label_names = sorted(labels.keys())

        t0 = time.monotonic()
        result = inject_labels(metrics, label_str, label_names)
        result += build_elapsed_gauge(all_labels)
        ts_ms = compute_timestamp_ms(all_labels)
        result = inject_timestamps(result, ts_ms)
        dt = time.monotonic() - t0

        self._send(result)
        safe_print(
            f"  scrape from {src}: {len(metrics)} -> {len(result)} bytes, inject {dt*1000:.1f}ms"
        )

    def _send(self, body):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if body:
            self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # suppress per-request logging


def resolve_bind_address(subnet_cidr):
    """Find the local IP address that belongs to the given subnet.

    Uses ``ip -j addr show`` to enumerate interfaces and returns the first
    address that falls within *subnet_cidr* (e.g. ``10.10.0.0/24``).
    """
    network = ipaddress.ip_network(subnet_cidr, strict=False)
    try:
        result = subprocess.run(
            ["ip", "-j", "addr", "show"],
            capture_output=True, text=True, check=True,
        )
        interfaces = json.loads(result.stdout)
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as exc:
        safe_print(f"Error: cannot enumerate interfaces: {exc}", stream=sys.stderr)
        sys.exit(1)

    for iface in interfaces:
        for addr_info in iface.get("addr_info", []):
            try:
                addr = ipaddress.ip_address(addr_info["local"])
            except (KeyError, ValueError):
                continue
            if addr in network:
                return str(addr)

    safe_print(f"Error: no interface address found in subnet {subnet_cidr}", stream=sys.stderr)
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Prometheus metrics proxy with label injection")
    parser.add_argument("--labels", default="/tmp/bench-metrics-labels.json",
                        help="Path to JSON file with labels to inject (default: /tmp/bench-metrics-labels.json)")
    parser.add_argument("--upstream", default="http://127.0.0.1:9100/",
                        help="Upstream reth metrics URL (default: http://127.0.0.1:9100/)")
    parser.add_argument("--target-metrics-config", default=None,
                        help="Target metrics config to periodically scrape into per-run files")
    parser.add_argument("--scrape-interval", type=float, default=1.0,
                        help="Seconds between background target-metric scrapes (default: 1.0)")

    bind_group = parser.add_mutually_exclusive_group()
    bind_group.add_argument("--bind", default=None,
                            help="Address to bind the proxy (default: 0.0.0.0)")
    bind_group.add_argument("--subnet", default=None,
                            help="Auto-detect bind address from a local interface in this subnet (e.g. 10.10.0.0/24)")

    parser.add_argument("--port", type=int, default=9090,
                        help="Port to bind the proxy (default: 9090)")
    args = parser.parse_args()

    configure_ci_process_lifecycle()

    if args.subnet:
        bind_addr = resolve_bind_address(args.subnet)
    elif args.bind:
        bind_addr = args.bind
    else:
        bind_addr = "0.0.0.0"

    server = HTTPServer((bind_addr, args.port), MetricsHandler)
    server.upstream = args.upstream
    server.labels_file = args.labels

    scraper = None
    if args.target_metrics_config:
        scraper = TargetMetricScraper(
            labels_file=args.labels,
            upstream=args.upstream,
            config_path=args.target_metrics_config,
            interval_s=args.scrape_interval,
        )
        scraper.start()

    safe_print(f"bench-metrics-proxy listening on {bind_addr}:{args.port}")
    safe_print(f"  upstream: {args.upstream}")
    safe_print(f"  labels:   {args.labels}")
    if args.target_metrics_config:
        safe_print(
            f"  target metrics: {args.target_metrics_config} ({args.scrape_interval:.2f}s interval)"
        )
    try:
        server.serve_forever()
    finally:
        if scraper is not None:
            scraper.stop()


if __name__ == "__main__":
    main()
