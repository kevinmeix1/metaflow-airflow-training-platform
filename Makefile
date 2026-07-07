.PHONY: demo run backfill plan-backfill dashboard policy-audit trace-report chaos-drill optimize-resources network-security gitops-plan dr-plan governance-bundle kubernetes-plan minikube-up test clean

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

chaos-drill:
	PYTHONPATH=src python3 -m training_orchestration_platform chaos-drill --output .local

optimize-resources:
	PYTHONPATH=src python3 -m training_orchestration_platform optimize-resources --output .local

network-security:
	PYTHONPATH=src python3 -m training_orchestration_platform network-security --output .local

gitops-plan:
	PYTHONPATH=src python3 -m training_orchestration_platform gitops-plan --output .local

dr-plan:
	PYTHONPATH=src python3 -m training_orchestration_platform dr-plan --output .local

governance-bundle:
	PYTHONPATH=src python3 -m training_orchestration_platform governance-bundle --output .local

kubernetes-plan:
	@find kubernetes gitops -name '*.yaml' -maxdepth 3 -print

minikube-up:
	@echo "Start Minikube and apply the training mesh workloads:"
	@echo "  minikube start --cpus=4 --memory=8192"
	@echo "  kubectl apply -f kubernetes/training-mesh-workloads.yaml"
	@echo "  kubectl apply -f kubernetes/resource-optimization.yaml"
	@echo "  kubectl apply -f kubernetes/network-security.yaml"
	@echo "  kubectl apply -f kubernetes/chaos-experiments.yaml"
	@echo "  kubectl apply -f kubernetes/disaster-recovery.yaml"
	@echo "  kubectl apply -f kubernetes/governance-evidence.yaml"
	@echo "  kubectl apply -f gitops/gitops-promotion.yaml"

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

clean:
	rm -rf .local
