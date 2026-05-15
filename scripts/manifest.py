import hashlib
import json
import logging
import os
from datetime import datetime, timezone

import duckdb

try:
    from .fileutils import log_step
except ImportError:
    from fileutils import log_step


def compute_sha256(path: str, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Stream-compute SHA-256 of a file without loading it into memory."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parquet_row_count(path: str) -> int:
    """Read row count from a parquet file using its metadata (fast, no full scan)."""
    return duckdb.sql(
        "SELECT COUNT(*) FROM read_parquet(?)", params=[path]
    ).fetchone()[0]


def write_manifest(
    logger: logging.Logger,
    dataset: str,
    parquet_dir: str,
    source: dict | None = None,
    schema_version: str = "v1",
    airflow_run_id: str | None = None,
    manifest_filename: str = "_manifest.json",
) -> str:
    """Generate `_manifest.json` describing every parquet file in `parquet_dir`.

    Per the platform spec, the manifest captures:
        - row count, size, SHA-256 per parquet file
        - upload (generation) timestamp
        - source file details (caller-supplied dict)
        - schema version
        - Airflow run id

    Reusable across datasets (NPPES, ICD-10, CPT, SNOMED).
    Returns the path to the written manifest file.
    """
    if not os.path.isdir(parquet_dir):
        raise FileNotFoundError(f"parquet dir does not exist: {parquet_dir}")

    manifest_path = os.path.join(parquet_dir, manifest_filename)

    with log_step(logger, f"build {dataset} {manifest_filename}"):
        files_info = []
        for entry in sorted(os.listdir(parquet_dir)):
            if not entry.endswith(".parquet"):
                continue
            full = os.path.join(parquet_dir, entry)
            logger.info(f"Hashing {entry} (~30-60s for multi-GB files)")
            sha = compute_sha256(full)
            rows = parquet_row_count(full)
            size = os.path.getsize(full)
            files_info.append({
                "filename": entry,
                "size_bytes": size,
                "sha256": sha,
                "row_count": rows,
            })
            logger.info(
                f"  -> {entry}: rows={rows}, size={size} bytes, sha256={sha[:16]}..."
            )

        manifest = {
            "dataset": dataset,
            "schema_version": schema_version,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "airflow_run_id": airflow_run_id,
            "files": files_info,
            "source": source or {},
        }

        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)

        logger.info(f"Wrote manifest: {manifest_path}")
        logger.info(f"Manifest contents:\n{json.dumps(manifest, indent=2)}")

    return manifest_path
