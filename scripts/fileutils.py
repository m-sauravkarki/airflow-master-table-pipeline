import logging
import os
import time
from contextlib import contextmanager
from functools import wraps


def get_logger(name: str) -> logging.Logger:
    """Return a logger that plays nicely with Airflow's task logging.

    Airflow attaches its own handlers when running inside a task; for standalone
    runs we fall back to a stream handler so logs still appear on the console.
    """
    logger = logging.getLogger(name)
    if not logger.handlers and not logging.getLogger().handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = True
    return logger


@contextmanager
def log_step(logger: logging.Logger, step_name: str):
    """Log entry, exit, and elapsed time for a named pipeline step."""
    logger.info(f"START :: {step_name}")
    start = time.perf_counter()
    try:
        yield
    except Exception as exc:
        elapsed = time.perf_counter() - start
        logger.exception(f"FAIL  :: {step_name} after {elapsed:.2f}s :: {exc}")
        raise
    else:
        elapsed = time.perf_counter() - start
        logger.info(f"DONE  :: {step_name} in {elapsed:.2f}s")


def log_call(logger: logging.Logger):
    """Decorator equivalent of log_step, keyed on the wrapped function's name."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with log_step(logger, func.__name__):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def human_size(num_bytes: float) -> str:
    """Render a byte count as a short human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num_bytes < 1024:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.2f} PB"


def log_file_info(logger: logging.Logger, path: str, label: str = "file") -> None:
    """Log path, size, and existence info for a file."""
    if not os.path.exists(path):
        logger.warning(f"{label} missing: {path}")
        return
    size = os.path.getsize(path)
    logger.info(f"{label}: {path} ({human_size(size)})")


def log_dir_contents(
    logger: logging.Logger,
    path: str,
    suffix: str | None = None,
    label: str = "dir",
) -> None:
    """Log the files in a directory, optionally filtered by suffix."""
    if not os.path.isdir(path):
        logger.warning(f"{label} missing: {path}")
        return
    entries = sorted(os.listdir(path))
    if suffix:
        entries = [e for e in entries if e.endswith(suffix)]
    if not entries:
        logger.info(f"{label}: {path} (empty)")
        return
    logger.info(f"{label}: {path} ({len(entries)} entries)")
    for entry in entries:
        full = os.path.join(path, entry)
        if os.path.isfile(full):
            logger.info(f"  - {entry} ({human_size(os.path.getsize(full))})")
        else:
            logger.info(f"  - {entry}/")
