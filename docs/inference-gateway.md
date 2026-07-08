# Gateway API Inference Extension

`make inference-gateway-plan` writes `.local/reports/inference_gateway_plan.json` and pairs it with `kubernetes/inference-gateway-routing.yaml`.

## What It Shows

- Stable v1 `InferencePool` routing for the promoted demand-forecast champion.
- Endpoint Picker integration with `FailOpen` fallback to the previous champion route.
- Alpha `InferenceObjective` examples for online forecasts and batch backtests.
- Gateway API `HTTPRoute` backend references that target an `InferencePool`.
- Training-owned release artifacts that serving teams can apply after promotion.

## Production Notes

The training platform should not become the serving control plane. It should publish the serving intent that follows from a successful training run: model identity, pool selector, endpoint-picker fallback, and route priorities. This keeps Airflow/Metaflow accountable for promotion evidence while letting the serving platform own runtime traffic.

References: Kubernetes Gateway API Inference Extension, InferencePool v1 docs, and Istio integration guide.
