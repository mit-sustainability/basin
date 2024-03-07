check-platform-env:
ifndef PLATFORM_ENV
	$(error PLATFORM_ENV is undefined)
endif

build-dagster-docker:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 860551228838.dkr.ecr.us-east-1.amazonaws.com && \
	docker buildx build --secret id=pgpass,src=.envrc --build-arg pg_host=${PG_WAREHOUSE_HOST} --build-arg pg_user=${PG_USER} -f orchestrator/Dockerfile -t ${IMAGE_TAG} --no-cache && \
	docker tag ${IMAGE_TAG} ${DAGSTER_IMAGE_ECR} && \
	docker push ${DAGSTER_IMAGE_ECR}

setup-dagster:
	cd orchestrator && pip install -r requirements.txt && pip install -e .

setup-dbt:
	cd warehouse && bash setup.sh

# Regenerate the dbt manifest.json
dbt_manifest:
	dbt parse \
		--project-dir warehouse \
		--profiles-dir warehouse

serve-dbt-catalog:
	cd warehouse && dbt docs generate && dbt docs serve

setup-dev:  setup-dbt  setup-dagster  # setup-libs setup-pants
	@echo "Done, enjoy building! 🎉"

run-tests:
	cd orchestrator && ./execute_unit_tests.sh
