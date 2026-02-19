# deployments/

Local and production deployment assets.

## Local stack

Start dependencies for API/coordinator development:

```bash
make stack-up
```

This launches:

- Postgres on `localhost:5432`
- MinIO on `localhost:9000` (`localhost:9001` console)

Default local credentials:

- Postgres: `postgres/postgres` (database `postgres`)
- MinIO: `minio/miniostorage`

Stop and remove volumes:

```bash
make dev-down
```

Stop full local stack without deleting volumes:

```bash
make stack-down
```

Optional demo workload generator:

```bash
make demo-up
make demo-status
```

Stop demo producer:

```bash
make demo-down
```

## Observability assets

Monitoring definitions are in `deployments/observability`:

- Prometheus rules: `deployments/observability/prometheus/duckmesh_rules.yaml`
- Prometheus recording rules: `deployments/observability/prometheus/duckmesh_recording_rules.yaml`
- Prometheus scrape example: `deployments/observability/prometheus/prometheus-scrape.example.yaml`
- Alertmanager routing example: `deployments/observability/alertmanager/alertmanager.example.yaml`
- Grafana dashboard: `deployments/observability/grafana/duckmesh_slo_dashboard.json`
