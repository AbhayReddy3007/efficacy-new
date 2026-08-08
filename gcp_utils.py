#!/usr/bin/env python3
"""
gcp_utils.py – Shared Google Cloud helpers for BigQuery and GCS operations.

All config is sourced from medical_potential.config.
Every other pipeline script (fetcher.py, push_to_bq.py,
generate_efficacy_report.py, clinical_efficacy.py) imports clients and
constants from here — never instantiate BQ/GCS clients elsewhere.

Quick check:
    python gcp_utils.py
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery, storage
from google.oauth2 import service_account
from medical_potential.config import (
    BQ_DATASET_ID,
    DIM_SCORES_TABLE,
    GCS_REPORT_BASE_PATH,
    GCS_BUCKET,
    GCS_MEDICAL_POTENTIAL_SUBFOLDER,
    GCS_PIPELINE_CACHE_BASE_PATH,
    GOOGLE_APPLICATION_CREDENTIALS,
    PROJECT_ID,
)

logger = logging.getLogger(__name__)

# Re-export so other files can do: from gcp_utils import PROJECT_ID, ...
__all__ = [
    "PROJECT_ID",
    "BQ_DATASET_ID",
    "DIM_SCORES_TABLE",
    "GCS_BUCKET",
    "GCS_REPORT_BASE_PATH",
    "GCS_MEDICAL_POTENTIAL_SUBFOLDER",
    "GCS_PIPELINE_CACHE_BASE_PATH",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "get_bq_client",
    "get_gcs_client",
    "upload_dimension_report_pdf_to_gcs",
    "upload_dimension_payload_cache_to_gcs",
    "append_dimension_score_to_bigquery",
    "full_bq_table",
    "gcs_path",
    "validate_config",
]

# These are not in medical_potential.config — set them here or via env
GEMINI_API_KEY:  str = os.getenv("GEMINI_API_KEY",        os.getenv("GOOGLE_API_KEY", ""))
GOOGLE_API_KEY:  str = os.getenv("GOOGLE_API_KEY",        "")
MODEL:           str = os.getenv("GEMINI_MODEL",           "gemini-2.5-flash")
RATIONALE_MODEL: str = os.getenv("GEMINI_RATIONALE_MODEL", "gemini-2.5-flash")

CLINICAL_EFFICACY_TABLE: str = os.getenv("CLINICAL_EFFICACY_TABLE", "clinical_efficacy")
PILLAR_SUBFOLDER = GCS_MEDICAL_POTENTIAL_SUBFOLDER

# ==============================================================================
# CLIENT HELPERS
# ==============================================================================

def get_bq_client() -> bigquery.Client:
    """Return an authenticated BigQuery client.

    Uses the configured service-account file when present; otherwise falls back
    to Application Default Credentials.
    """
    credentials_path = GOOGLE_APPLICATION_CREDENTIALS or "service.json"
    if credentials_path and os.path.exists(credentials_path):
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    return bigquery.Client(project=PROJECT_ID)


def get_gcs_client() -> storage.Client:
    """Return an authenticated GCS client.

    Uses the configured service-account file when present; otherwise falls back
    to Application Default Credentials.
    """
    credentials_path = GOOGLE_APPLICATION_CREDENTIALS
    if credentials_path and Path(credentials_path).exists():
        return storage.Client.from_service_account_json(credentials_path)
    return storage.Client(project=PROJECT_ID)


# ==============================================================================
# GCS UPLOADS
# ==============================================================================

def upload_dimension_report_pdf_to_gcs(
    pdf_bytes: bytes,
    molecule_name: str,
    dimension_name: str,
) -> tuple[str | None, str | None]:
    """Upload a dimension report PDF to GCS and return (main_uri, archived_uri)."""
    try:
        molecule = (molecule_name or "").strip()
        if not molecule:
            raise ValueError("molecule_name must be provided")

        client = get_gcs_client()
        bucket = client.bucket(GCS_BUCKET)

        main_gcs_path = f"{GCS_REPORT_BASE_PATH}/{molecule}/{PILLAR_SUBFOLDER}/{dimension_name}.pdf"
        main_blob = bucket.blob(main_gcs_path)
        main_blob.upload_from_string(pdf_bytes, content_type="application/pdf")
        main_gcs_uri = f"gs://{GCS_BUCKET}/{main_gcs_path}"
        logger.info("[REPORT_UPLOAD] PDF uploaded: %s", main_gcs_uri)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archived_gcs_path = (
            f"{GCS_REPORT_BASE_PATH}/{molecule}/{PILLAR_SUBFOLDER}/"
            f"archived/{dimension_name}_{timestamp}.pdf"
        )
        archived_blob = bucket.blob(archived_gcs_path)
        archived_blob.upload_from_string(pdf_bytes, content_type="application/pdf")
        archive_gcs_uri = f"gs://{GCS_BUCKET}/{archived_gcs_path}"
        logger.info("[REPORT_UPLOAD] Archived PDF uploaded: %s", archive_gcs_uri)

        return main_gcs_uri, archive_gcs_uri
    except Exception as exc:
        logger.warning("[REPORT_UPLOAD] Failed for '%s'/'%s': %s", molecule_name, dimension_name, exc)
        return None, None


def upload_dimension_payload_cache_to_gcs(
    payload: dict,
    molecule_name: str,
    dimension_name: str,
) -> str | None:
    """Upload a final dimension payload JSON to GCS and return the main cache URI."""
    try:
        molecule  = (molecule_name or "").strip()
        pillar    = (GCS_MEDICAL_POTENTIAL_SUBFOLDER or "").strip()
        dimension = (dimension_name or "").strip()
        if not molecule:
            raise ValueError("molecule_name must be provided")
        if not pillar:
            raise ValueError("GCS_MEDICAL_POTENTIAL_SUBFOLDER must be configured")
        if not dimension:
            raise ValueError("dimension_name must be provided")

        client = get_gcs_client()
        bucket = client.bucket(GCS_BUCKET)

        main_gcs_path  = f"{GCS_PIPELINE_CACHE_BASE_PATH}/{molecule}/{pillar}/{dimension}/output_payload.json"
        payload_bytes  = json.dumps(payload, indent=2, default=str).encode("utf-8")
        main_blob      = bucket.blob(main_gcs_path)
        main_blob.upload_from_string(payload_bytes, content_type="application/json")
        main_gcs_uri   = f"gs://{GCS_BUCKET}/{main_gcs_path}"
        logger.info("[CACHE_UPLOAD] Payload uploaded: %s", main_gcs_uri)

        timestamp          = datetime.now().strftime("%Y%m%d_%H%M%S")
        archived_gcs_path  = (
            f"{GCS_PIPELINE_CACHE_BASE_PATH}/{molecule}/{pillar}/{dimension}/"
            f"archived/output_payload_{timestamp}.json"
        )
        archived_blob = bucket.blob(archived_gcs_path)
        archived_blob.upload_from_string(payload_bytes, content_type="application/json")
        logger.info("[CACHE_UPLOAD] Archived payload: gs://%s/%s", GCS_BUCKET, archived_gcs_path)

        return main_gcs_uri
    except Exception as exc:
        logger.warning(
            "[CACHE_UPLOAD] Failed for '%s'/'%s'/'%s': %s",
            molecule_name, GCS_MEDICAL_POTENTIAL_SUBFOLDER, dimension_name, exc,
        )
        return None


# ==============================================================================
# BIGQUERY — DIM SCORES
# ==============================================================================

DIM_SCORES_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("product",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("pillar",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("dimension", "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("score",     "FLOAT",     mode="NULLABLE"),
    bigquery.SchemaField("rationale", "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
]


def append_dimension_score_to_bigquery(
    molecule_name: str,
    dimension_name: str,
    score: float | int | None,
    pillar_name: str = "Medical Potential",
    rationale: str | None = None,
) -> None:
    """Append one dimension-score row to the configured BigQuery dim_scores table."""
    table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{DIM_SCORES_TABLE}"
    row = {
        "product":   (molecule_name  or "").strip() or None,
        "pillar":    (pillar_name    or "").strip() or None,
        "dimension": (dimension_name or "").strip() or None,
        "score":     float(score) if score is not None else None,
        "rationale": (rationale    or "").strip() or None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    client     = get_bq_client()
    job_config = bigquery.LoadJobConfig(
        schema=DIM_SCORES_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    load_job = client.load_table_from_json([row], table_id, job_config=job_config)
    load_job.result()
    logger.info(
        "[DIM_SCORE] Appended score for '%s' / '%s' / '%s' to %s",
        row["product"], row["pillar"], row["dimension"], table_id,
    )


# ==============================================================================
# CONVENIENCE HELPERS
# ==============================================================================

def full_bq_table(table_name: str) -> str:
    """Return a fully-qualified BQ table ID: project.dataset.table."""
    return f"{PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"


def gcs_path(*parts: str) -> str:
    """Join GCS path segments cleanly."""
    return "/".join(p.strip("/") for p in parts if p)


def get_active_api_key() -> str:
    """Return whichever Gemini/Google API key is set."""
    return GEMINI_API_KEY or GOOGLE_API_KEY


def validate_config(raise_on_error: bool = True) -> list[str]:
    """Check that all required variables are set."""
    required = {
        "PROJECT_ID":                      PROJECT_ID,
        "GEMINI_API_KEY / GOOGLE_API_KEY": get_active_api_key(),
        "GCS_BUCKET":                      GCS_BUCKET,
    }
    missing = [name for name, val in required.items() if not val]
    if missing and raise_on_error:
        raise ValueError(
            "Missing required config (set in medical_potential.config or env):\n"
            + "\n".join(f"  - {m}" for m in missing)
        )
    return missing


# ==============================================================================
# SELF-CHECK  —  python gcp_utils.py
# ==============================================================================

if __name__ == "__main__":
    import sys

    W = 40
    print("\n── gcp_utils – Current Configuration ──────────────────────────")
    display = {
        "PROJECT_ID":                      PROJECT_ID                      or "(not set)",
        "GOOGLE_APPLICATION_CREDENTIALS":  GOOGLE_APPLICATION_CREDENTIALS  or "(using ADC)",
        "BQ_DATASET_ID":                   BQ_DATASET_ID,
        "CLINICAL_EFFICACY_TABLE":         CLINICAL_EFFICACY_TABLE,
        "DIM_SCORES_TABLE":                DIM_SCORES_TABLE,
        "GCS_BUCKET":                      GCS_BUCKET                      or "(not set)",
        "GCS_REPORT_BASE_PATH":            GCS_REPORT_BASE_PATH,
        "GCS_MEDICAL_POTENTIAL_SUBFOLDER": GCS_MEDICAL_POTENTIAL_SUBFOLDER,
        "GCS_PIPELINE_CACHE_BASE_PATH":    GCS_PIPELINE_CACHE_BASE_PATH,
        "GOOGLE_API_KEY":                  "***set***" if GOOGLE_API_KEY   else "(not set)",
        "GEMINI_API_KEY":                  "***set***" if GEMINI_API_KEY   else "(not set)",
        "MODEL":                           MODEL,
        "RATIONALE_MODEL":                 RATIONALE_MODEL,
    }
    for k, v in display.items():
        print(f"  {k:<{W}} {v}")
    print()

    missing = validate_config(raise_on_error=False)
    if missing:
        print("⚠  Missing required vars:", ", ".join(missing))
        sys.exit(1)
    print("✓  All required variables are set.\n")
