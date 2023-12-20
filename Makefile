check-platform-env:
ifndef PLATFORM_ENV
	$(error PLATFORM_ENV is undefined)
endif


## A way for quick patch before merging
# deploy-dagster:  check-platform-env
# 	@echo Deploying Dagster jobs to ${PLATFORM_ENV}

# 	@echo "Done! 🎉"


setup-dagster:
	cd orchestrator && pip install -r requirements.txt

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
