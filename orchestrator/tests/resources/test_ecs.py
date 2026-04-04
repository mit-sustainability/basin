import pytest

from dagster import Failure

from orchestrator.resources.ecs import (
    DEFAULT_CAPACITY_PROVIDER_STRATEGY,
    build_ecs_run_task_params,
    get_ecs_capacity_provider_strategy,
)


@pytest.fixture
def ecs_env(monkeypatch):
    monkeypatch.setenv("ECS_CLUSTER", "basin-cluster")
    monkeypatch.setenv("ECS_TASK_DEFINITION", "basin-task:1")
    monkeypatch.setenv("ECS_CONTAINER_NAME", "run")
    monkeypatch.setenv("ECS_SUBNET_IDS", "subnet-a,subnet-b")
    monkeypatch.setenv("ECS_SECURITY_GROUP_IDS", "sg-a,sg-b")
    monkeypatch.setenv("ECS_ASSIGN_PUBLIC_IP", "DISABLED")


def test_build_ecs_run_task_params_uses_spot_default(ecs_env):
    params = build_ecs_run_task_params(command=["python", "-m", "orchestrator.remote_tasks.website_content_health"])

    assert params["cluster"] == "basin-cluster"
    assert params["taskDefinition"] == "basin-task:1"
    assert params["capacityProviderStrategy"] == DEFAULT_CAPACITY_PROVIDER_STRATEGY
    assert params["networkConfiguration"]["awsvpcConfiguration"]["subnets"] == ["subnet-a", "subnet-b"]
    assert params["networkConfiguration"]["awsvpcConfiguration"]["securityGroups"] == ["sg-a", "sg-b"]
    assert params["overrides"]["containerOverrides"][0]["name"] == "run"
    assert params["overrides"]["containerOverrides"][0]["command"] == [
        "python",
        "-m",
        "orchestrator.remote_tasks.website_content_health",
    ]


def test_get_ecs_capacity_provider_strategy_uses_explicit_json(monkeypatch):
    monkeypatch.setenv(
        "ECS_CAPACITY_PROVIDER_STRATEGY",
        '[{"capacityProvider":"FARGATE_SPOT","weight":4},{"capacityProvider":"FARGATE","weight":1}]',
    )

    assert get_ecs_capacity_provider_strategy() == [
        {"capacityProvider": "FARGATE_SPOT", "weight": 4},
        {"capacityProvider": "FARGATE", "weight": 1},
    ]


def test_get_ecs_capacity_provider_strategy_rejects_invalid_json(monkeypatch):
    monkeypatch.setenv("ECS_CAPACITY_PROVIDER_STRATEGY", "not-json")

    with pytest.raises(Failure):
        get_ecs_capacity_provider_strategy()


def test_build_ecs_run_task_params_rejects_invalid_assign_public_ip(ecs_env, monkeypatch):
    monkeypatch.setenv("ECS_ASSIGN_PUBLIC_IP", "MAYBE")

    with pytest.raises(Failure):
        build_ecs_run_task_params(command=["python"])
