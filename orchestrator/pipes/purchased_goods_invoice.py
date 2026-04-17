from dagster_pipes import open_dagster_pipes

from orchestrator.assets.purchased_goods import (
    InvoiceConfig,
    _require_env,
    load_purchased_goods_invoice_data,
    write_purchased_goods_invoice_to_postgres,
)
from orchestrator.resources.datahub import DataHubResource


def main() -> None:
    with open_dagster_pipes() as pipes:
        files_to_download = pipes.get_extra("files_to_download")
        write_mode = pipes.get_extra("write_mode")
        datahub_api_key = _require_env("DATAHUB_API_KEY")
        invoice_config = InvoiceConfig(files_to_download=files_to_download, write_mode=write_mode)
        datahub = DataHubResource(auth_token=datahub_api_key)
        combined, metadata = load_purchased_goods_invoice_data(invoice_config, datahub)
        write_purchased_goods_invoice_to_postgres(combined, invoice_config.write_mode)
        pipes.report_asset_materialization(
            metadata={
                **metadata,
                "execution_mode": "ecs",
                "write_mode": invoice_config.write_mode,
            }
        )


if __name__ == "__main__":
    main()
