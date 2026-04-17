from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from dagster import ConfigurableResource, Failure


def load_sync_playwright():
    from playwright.sync_api import sync_playwright

    return sync_playwright


class PlaywrightBrowserResource(ConfigurableResource):
    headless: bool = True
    browser_name: str = "chromium"
    base_url: str | None = None
    accept_downloads: bool = False

    @contextmanager
    def browser_context(self) -> Iterator[object]:
        try:
            sync_playwright = load_sync_playwright()
        except ImportError as exc:
            raise Failure(
                "Playwright is required for website content health assets. "
                "Install the Python package and run `playwright install chromium`."
            ) from exc

        with sync_playwright() as playwright:
            browser_launcher = getattr(playwright, self.browser_name, None)
            if browser_launcher is None:
                raise Failure(f"Unsupported Playwright browser `{self.browser_name}`.")

            browser = browser_launcher.launch(headless=self.headless)
            context_kwargs = {}
            if self.base_url:
                context_kwargs["base_url"] = self.base_url
            if self.accept_downloads:
                context_kwargs["accept_downloads"] = True
            browser_context = browser.new_context(**context_kwargs)
            try:
                yield browser_context
            finally:
                browser_context.close()
                browser.close()
