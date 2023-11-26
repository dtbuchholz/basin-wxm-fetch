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
    handlers=[RichHandler(show_path=False, console=console)]
)
log = logging.getLogger("rich")


# Format unix timestamp (in milliseconds) to human readable string
def format_unix_ms(input: str) -> str:
    return datetime.utcfromtimestamp(
        input / 1000).strftime("%Y-%m-%d %H:%M:%S")


# Get current time as human readable string
def get_current_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


# Error handler to log error & raise exception
def err(desc: str, e: Exception, e_type: type = RuntimeError) -> None:
    if log_traceback is True:
        log.error(traceback.format_exc())
    log.error(f"{desc}: {e}")
    raise e_type(f"{desc}: {e}")


# Log errors without raising
def log_err(desc: str) -> None:
    if log_traceback is True:
        log.error(traceback.format_exc())
    log.error(f"{desc}")


# Log info
def log_info(desc: str) -> None:
    log.info(f"{desc}")


# Log warning
def log_warn(desc: str) -> None:
    if log_traceback is True:
        log.error(traceback.format_exc())
    log.warning(f"{desc}")


# Pretty print status of pending requests
def wrap_task(task: Callable[..., Any], desc: str) -> Any:
    with console.status(f"[bold green]{desc}") as status:
        start_time = time.time()
        result = task()
        end_time = time.time()
        duration = end_time - start_time
        log_info(f"{desc}done in {duration:.2f}s")
    return result
