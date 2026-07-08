.PHONY: demo run backfill plan-backfill dashboard policy-audit trace-report chaos-drill optimize-resources network-security gitops-plan dr-plan governance-bundle slo-report cloud-plan supply-chain orchestration-scorecard accelerator-plan device-plan topology-plan kuberay-plan inference-gateway-plan tenancy-report identity-report performance-budget queue-simulation release-admission ci-verify kubernetes-plan minikube-up test clean

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

slo-report:
	PYTHONPATH=src python3 -m training_orchestration_platform slo-report --output .local

cloud-plan:
	PYTHONPATH=src python3 -m training_orchestration_platform cloud-plan --output .local

supply-chain:
	PYTHONPATH=src python3 -m training_orchestration_platform supply-chain --output .local

orchestration-scorecard:
	PYTHONPATH=src python3 -m training_orchestration_platform orchestration-scorecard --output .local

accelerator-plan:
	PYTHONPATH=src python3 -m training_orchestration_platform accelerator-plan --output .local

device-plan:
	PYTHONPATH=src python3 -m training_orchestration_platform device-plan --output .local

topology-plan:
	PYTHONPATH=src python3 -m training_orchestration_platform topology-plan --output .local

kuberay-plan:
	PYTHONPATH=src python3 -m training_orchestration_platform kuberay-plan --output .local

inference-gateway-plan:
	PYTHONPATH=src python3 -m training_orchestration_platform inference-gateway-plan --output .local

tenancy-report:
	PYTHONPATH=src python3 -m training_orchestration_platform tenancy-report --output .local

identity-report:
	PYTHONPATH=src python3 -m training_orchestration_platform identity-report --output .local

performance-budget:
	PYTHONPATH=src python3 -m training_orchestration_platform performance-budget --output .local

queue-simulation:
	PYTHONPATH=src python3 -m training_orchestration_platform queue-simulation --output .local

release-admission:
	PYTHONPATH=src python3 -m training_orchestration_platform release-admission --output .local

ci-verify:
	PYTHONPATH=src python3 -m compileall -q src tests
	test -f .local/reports/training_orchestration_dashboard.html
	test -f .local/reports/index.html
	test -f .local/reports/governance_evidence_bundle.json
	test -f .local/reports/slo_error_budget.json
	test -f .local/reports/cloud_migration_plan.json
	test -f .local/reports/supply_chain_evidence.json
	test -f .local/reports/orchestration_scorecard.json
	test -f .local/reports/accelerator_capacity_plan.json
	test -f .local/reports/device_allocation_plan.json
	test -f .local/reports/topology_placement_plan.json
	test -f .local/reports/kuberay_capacity_plan.json
	test -f .local/reports/inference_gateway_plan.json
	test -f .local/reports/tenancy_fairness_report.json
	test -f .local/reports/identity_access_report.json
	test -f .local/reports/performance_budget.json
	test -f .local/reports/queue_simulation.json
	test -f .local/reports/release_admission_decision.json
	test -f .local/supply-chain/subject.checksums.txt
	python3 -m json.tool .local/reports/governance_evidence_bundle.json >/dev/null
	python3 -m json.tool .local/reports/slo_error_budget.json >/dev/null
	python3 -m json.tool .local/reports/cloud_migration_plan.json >/dev/null
	python3 -m json.tool .local/reports/supply_chain_evidence.json >/dev/null
	python3 -m json.tool .local/reports/orchestration_scorecard.json >/dev/null
	python3 -m json.tool .local/reports/accelerator_capacity_plan.json >/dev/null
	python3 -m json.tool .local/reports/device_allocation_plan.json >/dev/null
	python3 -m json.tool .local/reports/topology_placement_plan.json >/dev/null
	python3 -m json.tool .local/reports/kuberay_capacity_plan.json >/dev/null
	python3 -m json.tool .local/reports/inference_gateway_plan.json >/dev/null
	python3 -m json.tool .local/reports/tenancy_fairness_report.json >/dev/null
	python3 -m json.tool .local/reports/identity_access_report.json >/dev/null
	python3 -m json.tool .local/reports/performance_budget.json >/dev/null
	python3 -m json.tool .local/reports/queue_simulation.json >/dev/null
	python3 -m json.tool .local/reports/release_admission_decision.json >/dev/null

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
	@echo "  kubectl apply -f kubernetes/slo-alerts.yaml"
	@echo "  kubectl apply -f kubernetes/cloud-nodepools.yaml"
	@echo "  kubectl apply -f kubernetes/supply-chain-policy.yaml"
	@echo "  kubectl apply -f kubernetes/accelerator-scheduling.yaml"
	@echo "  kubectl apply -f kubernetes/dynamic-resource-allocation.yaml"
	@echo "  kubectl apply -f kubernetes/topology-aware-scheduling.yaml"
	@echo "  kubectl apply -f kubernetes/kuberay-kueue-workloads.yaml"
	@echo "  kubectl apply -f kubernetes/inference-gateway-routing.yaml"
	@echo "  kubectl apply -f kubernetes/multitenancy-fairness.yaml"
	@echo "  kubectl apply -f kubernetes/workload-identity.yaml"
	@echo "  kubectl apply -f kubernetes/performance-budget-policy.yaml"
	@echo "  kubectl apply -f kubernetes/queue-simulation-policy.yaml"
	@echo "  kubectl apply -f kubernetes/release-admission-policy.yaml"
	@echo "  kubectl apply -f gitops/gitops-promotion.yaml"

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

clean:
	rm -rf .local
