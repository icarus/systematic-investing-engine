"""Factor Engine: Chilean multi-factor equity research system."""

from importlib.metadata import version as _version

__all__ = ["get_version"]


def get_version() -> str:
    """Return the installed package version."""
    try:
        return _version("factor-engine")
    except Exception:  # pragma: no cover - fallback for editable installs
        return "0.0.0"
