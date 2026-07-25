#!/usr/bin/env python3
"""
eu_drug_trials.py – Unified script: queries CTIS + EudraCT, merges, dedupes.

    python eu_drug_trials.py "pembrolizumab"
    python eu_drug_trials.py "semaglutide" --details --results --strict
    python eu_drug_trials.py "aspirin" --source ctis --max-records 50

Requires: requests, beautifulsoup4
"""

import argparse, csv, json, re, sys
from typing import Any, Dict, List, Optional
import requests

import ctis_drug_trials as ctis
import eudract_drug_trials as eudract

EUDRACT_RE = re.compile(r"\b(\d{4}-\d{6}-\d{2})\b")


# ── Normalise both into a common schema ──────────────────────────────────────

def _find_eudract_number(summary, details):
    for blob in (summary, details):
        if not blob:
            continue
        for key in ("eudraCtCode", "eudractNumber", "eudraCtNumber"):
            value = blob.get(key)
            if value and EUDRACT_RE.search(str(value)):
                return EUDRACT_RE.search(str(value)).group(1)
        m = EUDRACT_RE.search(json.dumps(blob, default=str))
        if m:
            return m.group(1)
    return ""


def normalise_ctis(row, eudract_number=""):
    return {
        "source": "CTIS",
        "eudract_number": eudract_number,
        "ct_number": row.get("ct_number", ""),
        "title": row.get("title", ""),
        "short_title": row.get("short_title", ""),
        "status": row.get("status", ""),
        "phase": row.get("phase", ""),
        "sponsor": row.get("sponsors_full") or row.get("sponsor", ""),
        "sponsor_type": row.get("sponsor_type", ""),
        "sponsor_country": row.get("sponsor_countries", ""),
        "conditions": row.get("conditions", ""),
        "countries": row.get("countries", ""),
        "therapeutic_areas": row.get("therapeutic_areas", ""),
        "product": row.get("product_names") or row.get("product", ""),
        "active_substances": row.get("active_substances", ""),
        "atc_codes": row.get("atc_codes", ""),
        "pharmaceutical_form": row.get("pharmaceutical_forms", ""),
        "route": row.get("routes", ""),
        "strength": row.get("strengths", ""),
        "imp_role": row.get("imp_roles", ""),
        "orphan_drug": row.get("orphan_drug", ""),
        "has_marketing_auth": row.get("has_marketing_auth", ""),
        "population_age": row.get("age_group", ""),
        "gender": row.get("gender", ""),
        "enrolled": row.get("enrolled", ""),
        "planned_subjects_eea": row.get("planned_subjects_eea", ""),
        "planned_subjects_worldwide": row.get("planned_subjects_worldwide", ""),
        "main_objective": row.get("main_objective", ""),
        "secondary_objectives": row.get("secondary_objectives", ""),
        "primary_endpoint": row.get("primary_endpoint", ""),
        "secondary_endpoint": row.get("secondary_endpoint", ""),
        "inclusion_criteria": row.get("inclusion_criteria", ""),
        "exclusion_criteria": row.get("exclusion_criteria", ""),
        "trial_design": row.get("trial_design", ""),
        "comparator": row.get("comparator", ""),
        "start_date": row.get("start_date", ""),
        "end_date": row.get("end_date", ""),
        "decision_date": row.get("decision_date", ""),
        "results_available": row.get("results_first_received", ""),
        "last_updated": row.get("last_updated", ""),
        "findings": row.get("findings", ""),
        "ctis_url": row.get("url", ""),
        "eudract_url": "",
    }


def normalise_eudract(row):
    product = "; ".join(v for v in (row.get("product_name"), row.get("product_code")) if v)
    return {
        "source": "EudraCT",
        "eudract_number": row.get("eudract_number", ""),
        "ct_number": "",
        "title": row.get("full_title") or row.get("title", ""),
        "short_title": row.get("lay_title", ""),
        "status": row.get("end_of_trial_status") or row.get("status", ""),
        "phase": row.get("phase", ""),
        "sponsor": row.get("sponsor_name") or row.get("sponsor", ""),
        "sponsor_type": row.get("sponsor_status", ""),
        "sponsor_country": row.get("sponsor_country", ""),
        "conditions": row.get("medical_condition_full") or row.get("medical_condition", ""),
        "countries": row.get("countries", ""),
        "therapeutic_areas": row.get("therapeutic_area", ""),
        "product": product,
        "active_substances": row.get("inn", ""),
        "atc_codes": "",
        "pharmaceutical_form": row.get("pharmaceutical_form", ""),
        "route": row.get("route", ""),
        "strength": "; ".join(v for v in (row.get("strength"), row.get("strength_unit")) if v),
        "imp_role": row.get("imp_role", ""),
        "orphan_drug": row.get("orphan_drug", ""),
        "has_marketing_auth": row.get("has_marketing_auth", ""),
        "population_age": row.get("population_age", ""),
        "gender": row.get("gender", ""),
        "enrolled": row.get("results_subjects_worldwide")
                    or row.get("subjects_worldwide", ""),
        "planned_subjects_eea": row.get("subjects_eea", ""),
        "planned_subjects_worldwide": row.get("subjects_worldwide", ""),
        "main_objective": row.get("main_objective", ""),
        "secondary_objectives": row.get("secondary_objectives", ""),
        "primary_endpoint": row.get("primary_endpoint", ""),
        "secondary_endpoint": row.get("secondary_endpoint", ""),
        "inclusion_criteria": row.get("inclusion_criteria", ""),
        "exclusion_criteria": row.get("exclusion_criteria", ""),
        "trial_design": "; ".join(
            f for f in [
                "Randomised" if row.get("randomised", "").lower().startswith("yes") else "",
                "Double blind" if row.get("double_blind", "").lower().startswith("yes") else "",
                "Single blind" if row.get("single_blind", "").lower().startswith("yes") else "",
                "Open label" if row.get("open_label", "").lower().startswith("yes") else "",
                "Parallel" if row.get("parallel_group", "").lower().startswith("yes") else "",
                "Crossover" if row.get("crossover", "").lower().startswith("yes") else "",
                f"{row.get('number_of_arms', '')} arms" if row.get("number_of_arms") else "",
            ] if f),
        "comparator": "; ".join(
            f for f in [
                "Placebo" if row.get("comparator_placebo", "").lower().startswith("yes") else "",
                "Active" if row.get("comparator_other_product", "").lower().startswith("yes") else "",
            ] if f),
        "start_date": row.get("start_date", ""),
        "end_date": row.get("global_end_date", ""),
        "decision_date": row.get("ca_decision_date", ""),
        "results_available": row.get("results_available", ""),
        "last_updated": "",
        "findings": row.get("findings", ""),
        "ctis_url": "",
        "eudract_url": row.get("url", ""),
    }


def merge_rows(primary, secondary):
    merged = dict(primary)
    for key, value in secondary.items():
        if not merged.get(key) and value:
            merged[key] = value
        # For findings, concatenate if both have content
        if key == "findings" and merged.get(key) and value and value not in merged[key]:
            merged[key] = merged[key] + " ;; " + value
    merged["source"] = "CTIS + EudraCT"
    return merged


# ── Collection ───────────────────────────────────────────────────────────────
def collect_ctis(drug, args):
    session = requests.Session()
    session.headers.update(ctis.HEADERS)
    rows = []
    print("[CTIS] searching ...", file=sys.stderr)
    try:
        for i, summary in enumerate(
                ctis.search_trials(drug, session, page_size=50,
                                   max_records=args.max_records), start=1):
            details = None
            if args.details:
                try:
                    details = ctis.get_trial_details(summary.get("ctNumber", ""), session)
                except Exception as exc:
                    print(f"  ! CTIS details failed for {summary.get('ctNumber')}: {exc}",
                          file=sys.stderr)
            flat = ctis.flatten(summary, details)
            if args.strict and not ctis.mentions_drug(flat, drug):
                continue
            rows.append(normalise_ctis(flat, _find_eudract_number(summary, details)))
            if i % 25 == 0:
                print(f"  [CTIS] ...{i} processed", file=sys.stderr)
    except Exception as exc:
        print(f"[CTIS] ERROR: {exc}", file=sys.stderr)
    print(f"[CTIS] kept {len(rows)} trial(s)", file=sys.stderr)
    return rows


def collect_eudract(drug, args):
    session = requests.Session()
    session.headers.update(eudract.HEADERS)
    rows = []
    print("[EudraCT] searching ...", file=sys.stderr)
    try:
        for i, row in enumerate(
                eudract.search_trials(drug, session,
                                      max_records=args.max_records), start=1):
            if args.details:
                country = (row["countries"].split(";")[0] or "GB").strip()
                try:
                    row.update(eudract.get_trial_details(row["eudract_number"],
                                                         country, session))
                except Exception as exc:
                    print(f"  ! EudraCT details failed for {row['eudract_number']}: {exc}",
                          file=sys.stderr)
                time.sleep(eudract.POLITE_DELAY)

            if args.results and row.get("results_available") == "Yes":
                try:
                    row.update(eudract.get_trial_results(row["eudract_number"], session))
                except Exception as exc:
                    print(f"  ! EudraCT results failed for {row['eudract_number']}: {exc}",
                          file=sys.stderr)
                time.sleep(eudract.POLITE_DELAY)

            if args.strict and not eudract.mentions_drug(row, drug):
                continue
            rows.append(normalise_eudract(row))
            if i % 20 == 0:
                print(f"  [EudraCT] ...{i} processed", file=sys.stderr)
    except Exception as exc:
        print(f"[EudraCT] ERROR: {exc}", file=sys.stderr)
    print(f"[EudraCT] kept {len(rows)} trial(s)", file=sys.stderr)
    return rows


def deduplicate(ctis_rows, eudract_rows):
    by_eudract = {r["eudract_number"]: r for r in ctis_rows if r["eudract_number"]}
    merged = list(ctis_rows)
    matched = 0
    for row in eudract_rows:
        key = row["eudract_number"]
        if key and key in by_eudract:
            target = by_eudract[key]
            merged[merged.index(target)] = merge_rows(target, row)
            matched += 1
        else:
            merged.append(row)
    if matched:
        print(f"[merge] {matched} trial(s) present in both registers were combined",
              file=sys.stderr)
    return merged


# ── Output ───────────────────────────────────────────────────────────────────
COLUMNS = [
    "source", "eudract_number", "ct_number", "title", "short_title",
    "status", "phase", "sponsor", "sponsor_type", "sponsor_country",
    "conditions", "countries", "therapeutic_areas",
    "product", "active_substances", "atc_codes",
    "pharmaceutical_form", "route", "strength", "imp_role",
    "orphan_drug", "has_marketing_auth",
    "population_age", "gender", "enrolled",
    "planned_subjects_eea", "planned_subjects_worldwide",
    "main_objective", "secondary_objectives",
    "primary_endpoint", "secondary_endpoint",
    "inclusion_criteria", "exclusion_criteria",
    "trial_design", "comparator",
    "start_date", "end_date", "decision_date",
    "results_available", "last_updated",
    "findings",
    "ctis_url", "eudract_url",
]


def write_csv(rows, path):
    if not rows: return
    fieldnames = list(COLUMNS)
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(rows, path):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(rows, fh, indent=2, ensure_ascii=False)


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="Fetch EU clinical trials from CTIS + EudraCT, merged & de-duplicated.")
    ap.add_argument("drug", help="drug / active substance / trade name")
    ap.add_argument("--source", choices=("both", "ctis", "eudract"), default="both")
    ap.add_argument("--details", action="store_true",
                    help="Fetch full protocol / product info per trial")
    ap.add_argument("--results", action="store_true",
                    help="Fetch results/findings for completed trials (EudraCT)")
    ap.add_argument("--strict", action="store_true",
                    help="Keep only trials where the drug is in a product field")
    ap.add_argument("--max-records", type=int, default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--format", choices=("csv", "json", "both"), default="both")
    args = ap.parse_args()
    prefix = args.out or f"{args.drug.lower().replace(' ', '_')}_eu_trials"

    ctis_rows = collect_ctis(args.drug, args) if args.source in ("both", "ctis") else []
    eudract_rows = collect_eudract(args.drug, args) if args.source in ("both", "eudract") else []

    rows = deduplicate(ctis_rows, eudract_rows)
    if not rows:
        print("No trials found in either register.", file=sys.stderr)
        return 0

    if args.format in ("csv", "both"):
        write_csv(rows, f"{prefix}.csv")
        print(f"Wrote {len(rows)} trials -> {prefix}.csv", file=sys.stderr)
    if args.format in ("json", "both"):
        write_json(rows, f"{prefix}.json")
        print(f"Wrote {len(rows)} trials -> {prefix}.json", file=sys.stderr)

    print(f"\nTotal unique trials: {len(rows)}", file=sys.stderr)
    for row in rows[:5]:
        ident = row["ct_number"] or row["eudract_number"]
        ph = row.get("phase", "")
        print(f"  [{row['source']}] {ident}  Phase {ph}  {row['title'][:60]}",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
