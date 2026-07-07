.PHONY: demo run backfill dashboard test clean

demo:
	PYTHONPATH=src python3 -m training_orchestration_platform demo --output .local

run:
	PYTHONPATH=src python3 -m training_orchestration_platform run --output .local --date 2026-06-07

backfill:
	PYTHONPATH=src python3 -m training_orchestration_platform backfill --output .local --start 2026-06-01 --end 2026-06-05

dashboard:
	PYTHONPATH=src python3 -m training_orchestration_platform dashboard --output .local

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

clean:
	rm -rf .local
