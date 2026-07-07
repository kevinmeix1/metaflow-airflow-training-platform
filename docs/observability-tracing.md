# Observability And Tracing Layer

This repo now emits a local trace report and includes an OpenTelemetry Collector manifest.

## Commands

- `make trace-report` writes `.local/reports/trace_report.json`.
- `make demo` also emits the trace report.

## Trace Shape

The trace models a partition backfill across Airflow, Kueue, Metaflow, MLflow, and OpenLineage. Each span includes `trace_id`, `span_id`, `parent_span_id`, `service`, duration, status, and attributes.

## Cluster Mapping

`kubernetes/opentelemetry-collector.yaml` defines OTLP receivers, Kubernetes metadata enrichment, memory limiting, batch processing, Prometheus export, and debug trace export.
