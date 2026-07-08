"""Pytest configuration for wecom_stream plugin tests."""
import os
import sys
from pathlib import Path

# Ensure hermes-agent is importable
_HERMES_ROOT = Path(os.path.expanduser("~/.hermes/hermes-agent"))
_PLUGIN_DIR = Path(os.path.expanduser("~/.hermes/plugins/wecom_stream"))
for p in (_HERMES_ROOT, _HERMES_ROOT / "gateway", _PLUGIN_DIR):
    p_str = str(p)
    if p_str not in sys.path:
        sys.path.insert(0, p_str)

# Set test env vars
os.environ.setdefault("WECOM_STREAM_BOT_ID", "test_bot_123")
os.environ.setdefault("WECOM_STREAM_SECRET", "test_secret_456")
os.environ.setdefault("WECOM_DM_POLICY", "open")
os.environ.setdefault("WECOM_GROUP_POLICY", "open")
os.environ["WECOM_MEDIA_DIR"] = "/tmp/wecom_stream_test_media"

import pytest

# Auto async support
pytest_plugins = ('pytest_asyncio',)


def pytest_collection_modifyitems(config, items):
    """Add asyncio marker to all async tests."""
    for item in items:
        if asyncio.iscoroutinefunction(getattr(item, 'function', None)):
            item.add_marker(pytest.mark.asyncio)


import asyncio  # noqa: E402 — needed for the hook above
