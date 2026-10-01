from kymo._env import reject_legacy_client_env as _reject_legacy_client_env

_reject_legacy_client_env()
del _reject_legacy_client_env

from kymo._log import logger as _logger  # bind diagnostics before capture
from kymo._capture import install_capture as _install_capture

_install_capture()  # start capturing stdout/stderr immediately
del _logger

from kymo.client import (
    finish,
    init,
    is_initialized,
    log,
    log_cdn,
    open_run,
    run_url,
    update_config,
    wait_for_upload,
)

# After client, whose import wraps a stub mismatch in a clear ImportError.
from kymo.api import Api
from kymo.types import Image, Metadata, Resource

__all__ = [
    "Api",
    "finish",
    "init",
    "is_initialized",
    "log",
    "log_cdn",
    "open_run",
    "run_url",
    "update_config",
    "wait_for_upload",
    "Image",
    "Metadata",
    "Resource",
]
