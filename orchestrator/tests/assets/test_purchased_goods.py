from unittest.mock import MagicMock

import pytest

from orchestrator.assets.purchased_goods import (
    InvoiceConfig,
    PURCHASED_GOODS_INVOICE_ECS_COMMAND,
    run_purchased_goods_invoice_on_ecs,
)


@pytest.fixture
def ecs_env(monkeypatch):
    monkeypatch.setenv("ECS_CLUSTER", "basin-cluster")
    monkeypatch.setenv("ECS_TASK_DEFINITION", "basin-task:1")
    monkeypatch.setenv("ECS_CONTAINER_NAME", "run")
    monkeypatch.setenv("ECS_SUBNET_IDS", "subnet-a,subnet-b")
    monkeypatch.setenv("ECS_SECURITY_GROUP_IDS", "sg-a,sg-b")
    monkeypatch.setenv("ECS_ASSIGN_PUBLIC_IP", "DISABLED")


def test_invoice_config_defaults():
    config = InvoiceConfig()

    assert config.execution_mode == "local"
    assert config.write_mode == "append"


def test_run_purchased_goods_invoice_on_ecs(ecs_env):
    context = MagicMock()
    ecs_pipes_client = MagicMock()
    completed_invocation = ecs_pipes_client.run.return_value
    completed_invocation.get_materialize_result.return_value = "ecs-result"
    config = InvoiceConfig(files_to_download=["PurchasedGoods_Invoice_FY2025"], write_mode="replace")

    result = run_purchased_goods_invoice_on_ecs(context, config, ecs_pipes_client)

    assert result == "ecs-result"
    ecs_pipes_client.run.assert_called_once()
    call_kwargs = ecs_pipes_client.run.call_args.kwargs
    assert call_kwargs["context"] is context
    assert call_kwargs["extras"] == {
        "files_to_download": ["PurchasedGoods_Invoice_FY2025"],
        "write_mode": "replace",
    }
    assert (
        call_kwargs["run_task_params"]["overrides"]["containerOverrides"][0]["command"]
        == PURCHASED_GOODS_INVOICE_ECS_COMMAND
    )
