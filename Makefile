.PHONY: help test release-fixture check-release bootstrap-real-data

help:
	@echo "Use pnpm scripts for workspace tasks (pnpm build, pnpm test, pnpm dev:api, pnpm dev:worker)."
	@echo "Additional targets: make test, make release-fixture, make check-release, make bootstrap-real-data"

test:
	PYTHONPATH=src python3 -m unittest -q

release-fixture:
	python3 scripts/release_phase1.py --label fixture-release-001

check-release:
	python3 scripts/check_release_contract.py --release-manifest data/releases/fixture-release-001/release_manifest.json

bootstrap-real-data:
	python3 scripts/bootstrap_real_data.py --config configs/sources/phase1_sources.template.yaml
