#!/usr/bin/env python3
"""
clinical_efficacy.py – Full pipeline orchestrator for clinical efficacy.

Steps (per molecule):
  1. trial_fetcher.py  – Fetch raw trials from all registries → <molecule>_trials.json
  2. fetcher.py        – Enrich trials + compute efficacy score (in-memory, no Excel)
  3. push_to_bq.py     – Push enriched rows to BigQuery (incremental)
  4. generate_efficacy_report.py – Generate PDF report + upload to GCS

Accepts one or more molecule names.

Usage:
    python clinical_efficacy.py Semaglutide
    python clinical_efficacy.py Semaglutide Tirzepatide CagriSema
    python clinical_efficacy.py Semaglutide Tirzepatide --workers 8
    python clinical_efficacy.py CagriSema --skip-fetch
    python clinical_efficacy.py CagriSema --no-report
    python clinical_efficacy.py CagriSema --report-outdir ./reports

Programmatic use from another file:
    from clinical_efficacy import run_pipeline, run_pipeline_for_molecules

    run_pipeline("CagriSema")

    run_pipeline_for_molecules(["Semaglutide", "Tirzepatide", "CagriSema"])
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from typing import Optional


# ==============================================================================
# HELPERS
# ==============================================================================

def _run_trial_fetcher(
    molecule: str,
    max_records: Optional[int],
    top_n: Optional[int],
    out_json: str,
) -> None:
    """Run trial_fetcher.py to fetch raw trials. Exits on failure."""
    cmd = [sys.executable, "trial_fetcher.py", molecule, "--no-enrich", "--out", out_json]
    if max_records:
        cmd += ["--max-records", str(max_records)]
    if top_n:
        cmd += ["--top-n", str(top_n)]

    print(f"\n[CE] Step 1 – Fetching trials via trial_fetcher.py ...", file=sys.stderr)
    print(f"  > {' '.join(cmd)}\n", file=sys.stderr)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(f"ERROR: trial_fetcher.py failed for {molecule}. Aborting.")
    if not os.path.exists(out_json):
        sys.exit(f"ERROR: trial_fetcher.py ran but {out_json} was not created.")
    print(f"[CE] Trials written to {out_json}", file=sys.stderr)


# ==============================================================================
# SINGLE-MOLECULE PIPELINE
# ==============================================================================

def run_pipeline(
    molecule: str,
    max_records: Optional[int] = None,
    top_n: Optional[int] = None,
    workers: int = 6,
    no_score: bool = False,
    skip_fetch: bool = False,
    json_path: Optional[str] = None,  # explicit path to <molecule>_trials.json
    no_report: bool = False,
    report_outdir: Optional[str] = None,
) -> int:
    """
    Execute the full clinical efficacy pipeline for a single molecule.

    Returns 0 on success, non-zero on failure.
    """
    slug     = molecule.lower().replace(" ", "_")
    raw_json = json_path or f"{slug}_trials.json"
    t_start  = time.time()

    print(f"\n{'='*64}", file=sys.stderr)
    print(f"  CLINICAL EFFICACY PIPELINE  –  {molecule}", file=sys.stderr)
    print(f"{'='*64}", file=sys.stderr)

    # ── Step 1: Fetch raw trials (trial_fetcher.py) ───────────────────────
    if skip_fetch:
        if not os.path.exists(raw_json):
            print(f"[CE] ERROR: --skip-fetch set but {raw_json} not found.", file=sys.stderr)
            return 1
        print(f"\n[CE] Step 1 – Skipped (using {raw_json})", file=sys.stderr)
    else:
        _run_trial_fetcher(molecule, max_records, top_n, raw_json)

    # ── Step 2: Enrich + score (fetcher.py) ──────────────────────────────
    print(f"\n[CE] Step 2 – Enrichment + scoring (fetcher.py) ...", file=sys.stderr)
    try:
        import fetcher
    except ImportError:
        print("[CE] ERROR: fetcher.py not found.", file=sys.stderr)
        return 1

    enriched_rows, score_result, score_rationale = fetcher.run_fetcher(
        molecule    = molecule,
        json_path  = raw_json,
        max_workers = workers,
        no_score    = no_score,
    )

    if not enriched_rows:
        print("[CE] No enriched rows returned. Aborting.", file=sys.stderr)
        return 1

    print(f"\n[CE] Enrichment complete: {len(enriched_rows)} trial(s)", file=sys.stderr)
    if score_result:
        print(
            f"  Efficacy score : {score_result['weighted_score']} / 5.0\n"
            f"  Coverage       : {score_result['data_coverage']}",
            file=sys.stderr,
        )

    # ── Step 3: Push to BigQuery (push_to_bq.py) ─────────────────────────
    print(f"\n[CE] Step 3 – Pushing to BigQuery ...", file=sys.stderr)
    try:
        from push_to_bq import save_clinical_efficacy_to_bq
    except ImportError:
        print("[CE] ERROR: push_to_bq.py not found.", file=sys.stderr)
        return 1

    save_clinical_efficacy_to_bq(
        molecule_name = molecule,
        trials        = enriched_rows,
        score_result  = score_result,
        rationale     = score_rationale,
    )

    # Also append summary score to dim_scores table
    if score_result:
        try:
            from gcp_utils import append_dimension_score_to_bigquery
            append_dimension_score_to_bigquery(
                molecule_name  = molecule,
                dimension_name = "Clinical Efficacy",
                score          = score_result.get("weighted_score"),
                pillar_name    = "Medical Potential",
                rationale      = score_rationale,
            )
        except Exception as exc:
            print(f"[CE] Warning: could not append dim score: {exc}", file=sys.stderr)

    # ── Step 4: Generate PDF report (generate_efficacy_report.py) ────────
    if no_report:
        print(f"\n[CE] Step 4 – Skipped (--no-report)", file=sys.stderr)
    else:
        print(f"\n[CE] Step 4 – Generating efficacy report ...", file=sys.stderr)
        try:
            from generate_efficacy_report import generate_efficacy_report
        except ImportError:
            print("[CE] WARN: generate_efficacy_report.py not found – skipping.", file=sys.stderr)
        else:
            try:
                report_paths = generate_efficacy_report(
                    molecules = [molecule],
                    outdir    = report_outdir,
                )
                if report_paths:
                    print(
                        "[CE] Report(s) generated:\n"
                        + "\n".join(f"  {p}" for p in report_paths),
                        file=sys.stderr,
                    )
                else:
                    print(
                        "[CE] Report generation returned no output "
                        "(check GEMINI_API_KEY and BQ data).",
                        file=sys.stderr,
                    )
            except Exception as exc:
                print(f"[CE] WARN: Report generation failed: {exc}", file=sys.stderr)

    elapsed = time.time() - t_start
    print(
        f"\n{'='*64}\n"
        f"  DONE  –  {molecule}  ({elapsed:.1f}s)\n"
        f"{'='*64}\n",
        file=sys.stderr,
    )
    return 0


# ==============================================================================
# MULTI-MOLECULE PIPELINE
# ==============================================================================

def run_pipeline_for_molecules(
    molecules: list[str],
    max_records: Optional[int] = None,
    top_n: Optional[int] = None,
    workers: int = 6,
    no_score: bool = False,
    skip_fetch: bool = False,
    no_report: bool = False,
    report_outdir: Optional[str] = None,
) -> dict[str, int]:
    """
    Run the full pipeline for multiple molecules sequentially.

    Returns a dict of {molecule: return_code} for each molecule.
    A return_code of 0 means success.
    """
    results: dict[str, int] = {}
    total   = len(molecules)

    print(f"\n{'#'*64}", file=sys.stderr)
    print(f"  BATCH RUN: {total} molecule(s)", file=sys.stderr)
    print(f"  {', '.join(molecules)}", file=sys.stderr)
    print(f"{'#'*64}", file=sys.stderr)

    for idx, molecule in enumerate(molecules, start=1):
        print(f"\n[BATCH {idx}/{total}] Starting: {molecule}", file=sys.stderr)
        rc = run_pipeline(
            molecule      = molecule,
            max_records   = max_records,
            top_n         = top_n,
            workers       = workers,
            no_score      = no_score,
            skip_fetch    = skip_fetch,
            no_report     = no_report,
            report_outdir = report_outdir,
        )
        results[molecule] = rc
        status = "✓ OK" if rc == 0 else f"✗ FAILED (rc={rc})"
        print(f"[BATCH {idx}/{total}] {molecule}: {status}", file=sys.stderr)

    # Summary
    passed  = [m for m, rc in results.items() if rc == 0]
    failed  = [m for m, rc in results.items() if rc != 0]
    print(f"\n{'#'*64}", file=sys.stderr)
    print(f"  BATCH SUMMARY: {len(passed)}/{total} succeeded", file=sys.stderr)
    if failed:
        print(f"  Failed: {', '.join(failed)}", file=sys.stderr)
    print(f"{'#'*64}\n", file=sys.stderr)

    return results


# ==============================================================================
# CLI
# ==============================================================================

def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Clinical efficacy pipeline: fetch (trial_fetcher.py) → enrich + score "
            "(fetcher.py) → push to BigQuery (push_to_bq.py) → PDF report "
            "(generate_efficacy_report.py). Accepts one or more molecule names."
        )
    )
    ap.add_argument(
        "molecules",
        nargs="+",
        help="One or more molecule / drug names (e.g. Semaglutide Tirzepatide CagriSema)",
    )
    ap.add_argument("--max-records",   type=int, default=None,
                    help="Max records per registry (passed to trial_fetcher.py)")
    ap.add_argument("--top-n",         type=int, default=None,
                    help="Keep only top N trials by completeness")
    ap.add_argument("--workers",       type=int, default=6,
                    help="Concurrent Gemini enrichment workers (default: 6)")
    ap.add_argument("--no-score",      action="store_true",
                    help="Skip efficacy scoring step")
    ap.add_argument("--skip-fetch",    action="store_true",
                    help="Skip trial_fetcher.py; reuse existing <molecule>_trials.json")
    ap.add_argument("--no-report",     action="store_true",
                    help="Skip PDF report generation")
    ap.add_argument("--report-outdir", default=None,
                    help="Directory to save PDF reports (default: current directory)")
    args = ap.parse_args()

    molecules = [m.strip() for m in args.molecules if m.strip()]

    if len(molecules) == 1:
        return run_pipeline(
            molecule      = molecules[0],
            max_records   = args.max_records,
            top_n         = args.top_n,
            workers       = args.workers,
            no_score      = args.no_score,
            skip_fetch    = args.skip_fetch,
            no_report     = args.no_report,
            report_outdir = args.report_outdir,
        )
    else:
        results = run_pipeline_for_molecules(
            molecules     = molecules,
            max_records   = args.max_records,
            top_n         = args.top_n,
            workers       = args.workers,
            no_score      = args.no_score,
            skip_fetch    = args.skip_fetch,
            no_report     = args.no_report,
            report_outdir = args.report_outdir,
        )
        return 0 if all(rc == 0 for rc in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
