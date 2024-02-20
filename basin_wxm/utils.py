"""Utilities for formatting and logging."""

from datetime import datetime
from logging import INFO, basicConfig, getLogger
from time import time
from traceback import format_exc
from typing import Any, Callable

from rich.console import Console
from rich.logging import RichHandler

from .config import log_traceback

# Set up pretty logging
FORMAT = "%(message)s"
console = Console()
basicConfig(
    level=INFO,
    format=FORMAT,
    datefmt="[%X]",
    handlers=[RichHandler(show_path=False, console=console)],
)
log = getLogger("rich")


def unix_to_ms(unix: int) -> int:
    """Converts a unix timestamp to milliseconds."""
    return unix * 1000


def format_unix_ms(input: str) -> str:
    """Formats a unix timestamp (in milliseconds) to a human readable string."""
    return datetime.utcfromtimestamp(int(input) / 1000).strftime("%Y-%m-%d %H:%M:%S")


def get_current_date() -> str:
    """Gets the current date as a human readable string."""
    return datetime.now().strftime("%Y-%m-%d")


def to_title_case(input: str):
    """Converts a snake_case string to Title Case."""
    return " ".join(word.capitalize() for word in input.split("_"))


def err(message: str, e: Exception, e_type: type = RuntimeError) -> None:
    """Logs an error with a description and raises an exception."""
    if e is not None:
        message += f": {e}"
    if log_traceback is True:
        log.error(format_exc())

    raise e_type(message)


def log_err(message: str) -> None:
    """Logs an error with a description and traceback if enabled."""
    if log_traceback is True:
        log.error(format_exc())
    log.error(f"{message}")


def log_info(desc: str) -> None:
    """Logs an info message with a description."""
    log.info(f"{desc}")


def log_warn(desc: str) -> None:
    """Logs a warning with a description and traceback if enabled."""
    if log_traceback is True:
        log.error(format_exc())
    log.warning(f"{desc}")


def wrap_task(task: Callable[..., Any], desc: str) -> Any:
    """Wraps a function with a description and logs the duration of the task."""
    with console.status(f"[bold green]{desc}"):
        start_time = time()
        result = task()
        end_time = time()
        duration = end_time - start_time
        log_info(f"{desc}done in {duration:.2f}s")
    return result
