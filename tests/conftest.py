import asyncio
import os

import pytest
from django.conf import settings


def pytest_configure():
    os.environ["DJANGO_SETTINGS_MODULE"] = "tests.sample_project.config.settings"
    settings._setup()


def pytest_generate_tests(metafunc):
    if "samesite" in metafunc.fixturenames:
        metafunc.parametrize("samesite", ["Strict", "None"], indirect=True)


@pytest.fixture(autouse=True)
async def keep_event_loop_awake():
    """
    Keeps the event loop waking up periodically during async tests.

    Sandboxed environments may block the self-pipe write that wakes an event
    loop when work is completed from another thread (e.g. by
    database_sync_to_async or a sync consumer's thread); a short periodic
    wakeup ensures those callbacks are still processed. Harmless elsewhere.
    """
    async def pulse():
        while True:
            await asyncio.sleep(0.01)

    task = asyncio.ensure_future(pulse())
    yield
    task.cancel()


@pytest.fixture
def samesite(request, settings):
    """Set samesite flag to strict."""
    settings.SESSION_COOKIE_SAMESITE = request.param


@pytest.fixture
def samesite_invalid(settings):
    """Set samesite flag to strict."""
    settings.SESSION_COOKIE_SAMESITE = "Hello"
