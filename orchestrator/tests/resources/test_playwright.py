from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dagster import Failure

from orchestrator.resources.playwright import PlaywrightBrowserResource


def test_browser_context_yields_new_context():
    resource = PlaywrightBrowserResource(base_url="https://example.com")
    mock_context = MagicMock()
    mock_browser = MagicMock()
    mock_browser.new_context.return_value = mock_context
    mock_launcher = MagicMock()
    mock_launcher.launch.return_value = mock_browser
    mock_playwright = MagicMock(chromium=mock_launcher)
    mock_manager = MagicMock()
    mock_manager.__enter__.return_value = mock_playwright
    mock_manager.__exit__.return_value = None

    with patch("orchestrator.resources.playwright.load_sync_playwright", return_value=lambda: mock_manager):
        with resource.browser_context() as browser_context:
            assert browser_context is mock_context

    mock_launcher.launch.assert_called_once_with(headless=True)
    mock_browser.new_context.assert_called_once_with(base_url="https://example.com")
    mock_context.close.assert_called_once()
    mock_browser.close.assert_called_once()


def test_browser_context_rejects_unknown_browser():
    resource = PlaywrightBrowserResource(browser_name="webkit2")
    mock_playwright = SimpleNamespace()
    mock_manager = MagicMock()
    mock_manager.__enter__.return_value = mock_playwright
    mock_manager.__exit__.return_value = None

    with patch("orchestrator.resources.playwright.load_sync_playwright", return_value=lambda: mock_manager):
        with pytest.raises(Failure):
            with resource.browser_context():
                pass
