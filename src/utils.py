"""Utilities for formatting and logging."""

import time
from typing import Any, Callable
from datetime import datetime as dt
import logging
from rich.console import Console
from rich.logging import RichHandler

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
def format_unix(input: str) -> str:
    return dt.utcfromtimestamp(
        input / 1000).strftime("%Y-%m-%d %H:%M:%S")

# Error handler to print error


def err(desc: str, e: Exception, e_type: type = RuntimeError) -> None:
    log.error(f"{desc}: {e}")
    raise e_type(f"{desc}: {e}")


# Log errors without raising
def log_err(desc: str) -> None:
    log.error(f"{desc}")


# Log info
def log_info(desc: str) -> None:
    log.info(f"{desc}")


# Pretty print status of pending requests
def wrap_task(task: Callable[..., Any], desc: str) -> Any:
    with console.status(f"[bold green]{desc}") as status:
        start_time = time.time()
        result = task()
        end_time = time.time()
        duration = end_time - start_time
        log_info(f"{desc}done in {duration:.2f}s")
    return result
