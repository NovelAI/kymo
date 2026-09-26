"""Logger for kymo.

Verbose (INFO) output is gated behind ``KYMO_VERBOSE``.
Warnings and errors are always emitted to stderr. Downstream code can
re-configure the ``kymo`` logger to integrate with its own logging
setup if needed.
"""

import logging
import sys

from kymo._env import boolean as _env_boolean


def _verbose_env() -> bool:
    # verbose logging must never make importing the client fail.
    return _env_boolean("KYMO_VERBOSE", "PYMKDB2_VERBOSE", strict=False)


logger = logging.getLogger("kymo")

if not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("kymo: %(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO if _verbose_env() else logging.WARNING)
    logger.propagate = False
