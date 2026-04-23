import importlib
import sys

import boto3

from orchestrator.resources.confluence import ConfluenceResource


def test_importing_orchestrator_does_not_create_lambda_client(monkeypatch):
    original_module = sys.modules.pop("orchestrator", None)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("boto3.client should not be called during orchestrator import")

    monkeypatch.setattr(boto3, "client", fail_if_called)

    try:
        importlib.import_module("orchestrator")
    finally:
        sys.modules.pop("orchestrator", None)
        if original_module is not None:
            sys.modules["orchestrator"] = original_module


def test_orchestrator_definitions_register_confluence_resource():
    import orchestrator

    confluence_resource = orchestrator.defs.resources["confluence"]

    assert isinstance(confluence_resource, ConfluenceResource)
    assert confluence_resource.base_url == "https://wikis.mit.edu/confluence"
    assert confluence_resource.space_key == "MITOS"
