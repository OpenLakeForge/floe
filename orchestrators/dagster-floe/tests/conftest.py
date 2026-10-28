from __future__ import annotations

import sys

import pytest


@pytest.fixture(autouse=True)
def _no_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make s3:// reads take the fsspec path unless a test installs a fake boto3."""
    monkeypatch.setitem(sys.modules, "boto3", None)
