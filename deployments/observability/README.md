# Observability Assets

This directory contains production-oriented monitoring artifacts for DuckMesh.

## Files

- `prometheus/duckmesh_rules.yaml`
  - Prometheus alerting rules for key DuckMesh SLO and integrity signals.
- `prometheus/duckmesh_recording_rules.yaml`
  - Prometheus recording rules for DuckMesh SLO rollups used by alerts/dashboards.
- `prometheus/prometheus-scrape.example.yaml`
  - Example Prometheus scrape config and `rule_files` wiring for DuckMesh API metrics.
- `alertmanager/alertmanager.example.yaml`
  - Example Alertmanager routing grouped by severity/service with receiver stubs.
- `grafana/duckmesh_slo_dashboard.json`
  - Grafana dashboard definition covering ingest latency, write-to-visible, lag, consistency timeouts, and integrity failures.

## Usage

1. Copy `prometheus/duckmesh_recording_rules.yaml` and `prometheus/duckmesh_rules.yaml` into your Prometheus rules directory.
2. Import `grafana/duckmesh_slo_dashboard.json` into Grafana.
3. Start Alertmanager with `alertmanager/alertmanager.example.yaml` as a baseline and replace receiver URLs.
4. Ensure Prometheus scrapes DuckMesh API `/v1/metrics` and that Grafana datasource points to that Prometheus.
5. Tune alert thresholds for your environment and traffic profile before production paging.

## Notes

- Alert thresholds are aligned with initial SLO targets in `docs/OPERATIONS.md`.
- Rules are service-level by default and not tenant-labeled.
