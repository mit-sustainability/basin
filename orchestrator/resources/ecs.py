from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from typing import Any

from dagster import Failure


DEFAULT_CAPACITY_PROVIDER_STRATEGY = [{"capacityProvider": "FARGATE_SPOT", "weight": 1}]
DEFAULT_ASSIGN_PUBLIC_IP = "DISABLED"


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise Failure(f"Missing required ECS environment variable `{name}`.")


def _parse_csv_env(name: str) -> list[str]:
    raw_value = _require_env(name)
    values = [value.strip() for value in raw_value.split(",") if value.strip()]
    if values:
        return values
    raise Failure(f"ECS environment variable `{name}` must contain at least one value.")


def _parse_capacity_provider_strategy(raw_value: str | None) -> list[dict[str, Any]]:
    if not raw_value:
        return list(DEFAULT_CAPACITY_PROVIDER_STRATEGY)

    try:
        parsed_value = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise Failure("`ECS_CAPACITY_PROVIDER_STRATEGY` must be valid JSON.") from exc

    if not isinstance(parsed_value, list) or not parsed_value:
        raise Failure("`ECS_CAPACITY_PROVIDER_STRATEGY` must be a non-empty JSON array.")

    for index, provider in enumerate(parsed_value):
        if not isinstance(provider, dict):
            raise Failure(
                f"`ECS_CAPACITY_PROVIDER_STRATEGY[{index}]` must be an object, got {type(provider).__name__}."
            )
        if not provider.get("capacityProvider"):
            raise Failure(f"`ECS_CAPACITY_PROVIDER_STRATEGY[{index}]` must include `capacityProvider`.")

    return parsed_value


def get_ecs_capacity_provider_strategy() -> list[dict[str, Any]]:
    return _parse_capacity_provider_strategy(os.getenv("ECS_CAPACITY_PROVIDER_STRATEGY"))


def build_ecs_run_task_params(
    *,
    command: Sequence[str],
    environment: Sequence[Mapping[str, str]] | None = None,
) -> dict[str, Any]:
    assign_public_ip = os.getenv("ECS_ASSIGN_PUBLIC_IP", DEFAULT_ASSIGN_PUBLIC_IP).upper()
    if assign_public_ip not in {"ENABLED", "DISABLED"}:
        raise Failure("`ECS_ASSIGN_PUBLIC_IP` must be either `ENABLED` or `DISABLED`.")

    container_name = _require_env("ECS_CONTAINER_NAME")
    return {
        "cluster": _require_env("ECS_CLUSTER"),
        "taskDefinition": _require_env("ECS_TASK_DEFINITION"),
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": _parse_csv_env("ECS_SUBNET_IDS"),
                "securityGroups": _parse_csv_env("ECS_SECURITY_GROUP_IDS"),
                "assignPublicIp": assign_public_ip,
            }
        },
        "capacityProviderStrategy": get_ecs_capacity_provider_strategy(),
        "overrides": {
            "containerOverrides": [
                {
                    "name": container_name,
                    "command": list(command),
                    "environment": [dict(item) for item in environment] if environment else [],
                }
            ]
        },
    }
