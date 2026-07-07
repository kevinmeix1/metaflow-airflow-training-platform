.PHONY: demo run backfill plan-backfill dashboard policy-audit trace-report kubernetes-plan minikube-up test clean

demo:
	PYTHONPATH=src python3 -m training_orchestration_platform demo --output .local

run:
	PYTHONPATH=src python3 -m training_orchestration_platform run --output .local --date 2026-06-07

backfill:
	PYTHONPATH=src python3 -m training_orchestration_platform backfill --output .local --start 2026-06-01 --end 2026-06-05

plan-backfill:
	PYTHONPATH=src python3 -m training_orchestration_platform plan-backfill --output .local --start 2026-06-01 --end 2026-06-07

dashboard:
	PYTHONPATH=src python3 -m training_orchestration_platform dashboard --output .local

policy-audit:
	PYTHONPATH=src python3 -m training_orchestration_platform policy-audit --output .local

trace-report:
	PYTHONPATH=src python3 -m training_orchestration_platform trace-report --output .local

kubernetes-plan:
	@find kubernetes -name '*.yaml' -maxdepth 3 -print

minikube-up:
	@echo "Start Minikube and apply the training mesh workloads:"
	@echo "  minikube start --cpus=4 --memory=8192"
	@echo "  kubectl apply -f kubernetes/training-mesh-workloads.yaml"

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

clean:
	rm -rf .local
