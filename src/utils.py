"""Utilities for formatting and logging."""

import logging
import time
import traceback
from datetime import datetime
from typing import Any, Callable

from rich.console import Console
from rich.logging import RichHandler

from config import log_traceback


# Set up pretty logging
FORMAT = "%(message)s"
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format=FORMAT,
    datefmt="[%X]",
    handlers=[RichHandler(show_path=False, console=console)],
)
log = logging.getLogger("rich")


def format_unix_ms(input: str) -> str:
    """Formats a unix timestamp (in milliseconds) to a human readable string."""
    return datetime.utcfromtimestamp(input / 1000).strftime("%Y-%m-%d %H:%M:%S")


def get_current_date() -> str:
    """Gets the current date as a human readable string."""
    return datetime.now().strftime("%Y-%m-%d")


def is_pinata(url: str) -> bool:
    """Returns true if the URL is a Pinata gateway URL (used in logging info)."""
    return True if "mypinata.cloud" in url else False


def err(desc: str, e: Exception, e_type: type = RuntimeError) -> None:
    """Logs an error with a description and raises an exception."""
    if log_traceback is True:
        log.error(traceback.format_exc())
    log.error(f"{desc}: {e}")
    raise e_type(f"{desc}: {e}")


def log_err(desc: str) -> None:
    """Logs an error with a description and traceback if enabled."""
    if log_traceback is True:
        log.error(traceback.format_exc())
    log.error(f"{desc}")


def log_info(desc: str) -> None:
    """Logs an info message with a description."""
    log.info(f"{desc}")


def log_warn(desc: str) -> None:
    """Logs a warning with a description and traceback if enabled."""
    if log_traceback is True:
        log.error(traceback.format_exc())
    log.warning(f"{desc}")


def wrap_task(task: Callable[..., Any], desc: str) -> Any:
    """Wraps a function with a description and logs the duration of the task."""
    with console.status(f"[bold green]{desc}") as status:
        start_time = time.time()
        result = task()
        end_time = time.time()
        duration = end_time - start_time
        log_info(f"{desc}done in {duration:.2f}s")
    return result
