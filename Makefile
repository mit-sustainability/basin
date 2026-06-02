check-platform-env:
ifndef PLATFORM_ENV
	$(error PLATFORM_ENV is undefined)
endif

build-dagster-docker:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 860551228838.dkr.ecr.us-east-1.amazonaws.com && \
	docker buildx build --secret id=pgpass,src=.envrc --build-arg pg_host=${PG_WAREHOUSE_HOST} --build-arg pg_user=${PG_USER} -f orchestrator/Dockerfile -t ${IMAGE_TAG} --no-cache && \
	docker tag ${IMAGE_TAG} ${DAGSTER_IMAGE_ECR} && \
	docker push ${DAGSTER_IMAGE_ECR}

setup-python:
	uv sync --all-groups

setup-playwright:
	uv run playwright install chromium

setup-dbt:
	uv run dbt deps --project-dir warehouse --profiles-dir warehouse
	mkdir -p ~/.dbt && if [ ! -f ~/.dbt/profiles.yml ]; then cp warehouse/profiles.yml ~/.dbt/profiles.yml; fi
# Regenerate the dbt manifest.json
dbt_manifest:
	uv run dbt parse \
		--project-dir warehouse \
		--profiles-dir warehouse

serve-dbt-catalog:
	uv run dbt docs generate --project-dir warehouse && uv run dbt docs serve --project-dir warehouse

setup-dev: setup-python setup-dbt setup-playwright
	@echo "Done, enjoy building!"

run-tests:
	cd orchestrator && ./execute_unit_tests.sh
