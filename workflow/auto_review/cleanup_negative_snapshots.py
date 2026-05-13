#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
AUTO_REVIEW_FINAL_DIR = PROJECT_ROOT / "data" / "auto_review" / "final" / "by_instance"
REPO_SNAPSHOTS_DIR = PROJECT_ROOT / "data" / "raw" / "repo_snapshots"
REPO_SNAPSHOT_ARCHIVES_DIR = PROJECT_ROOT / "data" / "raw" / "repo_snapshot_archives"
SNAPSHOT_PULL_LIST_PATH = REPO_SNAPSHOT_ARCHIVES_DIR / "snapshot_pull_list.json"
SNAPSHOT_CLONE_LIST_PATH = REPO_SNAPSHOT_ARCHIVES_DIR / "snapshot_clone_list.json"


def load_json(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8-sig", errors="replace")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw, strict=False)


def normalize_archive_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "instance_id": str(manifest.get("instance_id", "")).strip(),
        "subtype": str(manifest.get("subtype", "")).strip(),
        "repo_full_name": str(manifest.get("repo_full_name", "")).strip(),
        "pr_number": manifest.get("pr_number", 0),
        "snapshot_root_relpath": str(manifest.get("snapshot_root_relpath", "")).strip(),
        "restore_parent_relpath": str(manifest.get("restore_parent_relpath", "")).strip(),
        "archive_parts": [str(item).strip() for item in manifest.get("archive_parts", [])],
        "manifest_relpath": str(manifest.get("manifest_relpath", "")).strip(),
        "created_at": str(manifest.get("created_at", "")).strip(),
        "github_safe": bool(manifest.get("github_safe", False)),
        "archive_format": str(manifest.get("archive_format", "zip")).strip() or "zip",
    }


def rebuild_snapshot_pull_list() -> None:
    entries: list[dict[str, Any]] = []
    for manifest_path in sorted(REPO_SNAPSHOT_ARCHIVES_DIR.glob("*/*/manifest.json")):
        try:
            manifest = load_json(manifest_path)
        except Exception:
            continue
        if not manifest.get("instance_id"):
            continue
        entries.append(normalize_archive_manifest(manifest))
    payload = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "entries": entries,
    }
    with SNAPSHOT_PULL_LIST_PATH.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def collect_auto_review_stats() -> dict[str, Counter]:
    stats: dict[str, Counter] = defaultdict(Counter)
    for path in sorted(AUTO_REVIEW_FINAL_DIR.glob("*.json")):
        payload = load_json(path)
        subtype = str(payload.get("subtype") or path.stem.split("__", 1)[0]).strip()
        label = str(payload.get("manual_label") or "").strip().lower() or "unknown"
        stats[subtype]["total"] += 1
        stats[subtype][label] += 1
    return stats


def find_snapshot_dirs_for_instance(instance_id: str, subtype: str) -> list[Path]:
    subtype_root = REPO_SNAPSHOTS_DIR / subtype
    if not subtype_root.exists():
        return []
    safe_matches: list[Path] = []
    for child in subtype_root.iterdir():
        if not child.is_dir():
            continue
        if instance_id not in child.name:
            continue
        resolved = child.resolve()
        try:
            resolved.relative_to(subtype_root.resolve())
        except ValueError:
            continue
        safe_matches.append(resolved)
    return sorted(safe_matches)


def find_archive_dirs_for_instance(instance_id: str, subtype: str) -> list[Path]:
    subtype_root = REPO_SNAPSHOT_ARCHIVES_DIR / subtype
    if not subtype_root.exists():
        return []
    safe_matches: list[Path] = []
    bundle_dir = subtype_root / instance_id
    if bundle_dir.exists() and bundle_dir.is_dir():
        resolved = bundle_dir.resolve()
        try:
            resolved.relative_to(subtype_root.resolve())
        except ValueError:
            return []
        safe_matches.append(resolved)
    return safe_matches


def collect_labeled_instances(label: str, subtype: str = "", limit: int = 0) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for path in sorted(AUTO_REVIEW_FINAL_DIR.glob("*.json")):
        payload = load_json(path)
        payload_subtype = str(payload.get("subtype") or path.stem.split("__", 1)[0]).strip()
        payload_label = str(payload.get("manual_label") or "").strip().lower()
        if subtype and payload_subtype != subtype:
            continue
        if payload_label != label:
            continue
        snapshot_dirs = find_snapshot_dirs_for_instance(path.stem, payload_subtype)
        matches.append(
            {
                "instance_id": path.stem,
                "subtype": payload_subtype,
                "manual_label": payload_label,
                "snapshot_dirs": [str(item) for item in snapshot_dirs],
                "archive_dirs": [str(item) for item in find_archive_dirs_for_instance(path.stem, payload_subtype)],
            }
        )
        if limit > 0 and len(matches) >= limit:
            break
    return matches


def delete_snapshot_dirs(items: list[dict[str, Any]], apply: bool) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in items:
        deleted_snapshot_dirs: list[str] = []
        deleted_archive_dirs: list[str] = []
        would_delete_snapshot_dirs: list[str] = []
        would_delete_archive_dirs: list[str] = []
        missing_snapshot_dirs: list[str] = []
        missing_archive_dirs: list[str] = []
        for raw_path in item["snapshot_dirs"]:
            path = Path(raw_path)
            if not path.exists():
                missing_snapshot_dirs.append(str(path))
                continue
            if apply:
                shutil.rmtree(path)
                deleted_snapshot_dirs.append(str(path))
            else:
                would_delete_snapshot_dirs.append(str(path))
        for raw_path in item["archive_dirs"]:
            path = Path(raw_path)
            if not path.exists():
                missing_archive_dirs.append(str(path))
                continue
            if apply:
                shutil.rmtree(path)
                deleted_archive_dirs.append(str(path))
            else:
                would_delete_archive_dirs.append(str(path))
        results.append(
            {
                "instance_id": item["instance_id"],
                "subtype": item["subtype"],
                "manual_label": item["manual_label"],
                "matched_snapshot_dirs": item["snapshot_dirs"],
                "matched_archive_dirs": item["archive_dirs"],
                "would_delete_snapshot_dirs": would_delete_snapshot_dirs,
                "would_delete_archive_dirs": would_delete_archive_dirs,
                "deleted_snapshot_dirs": deleted_snapshot_dirs,
                "deleted_archive_dirs": deleted_archive_dirs,
                "missing_snapshot_dirs": missing_snapshot_dirs,
                "missing_archive_dirs": missing_archive_dirs,
                "applied": apply,
            }
        )
    return results


def update_list_file(path: Path, key: str, instance_ids_to_remove: set[str]) -> None:
    if not path.exists():
        return
    if path == SNAPSHOT_PULL_LIST_PATH:
        try:
            payload = load_json(path)
        except Exception:
            rebuild_snapshot_pull_list()
            payload = load_json(path)
    else:
        payload = load_json(path)
    entries = payload.get(key, [])
    filtered = [entry for entry in entries if str(entry.get("instance_id", "")).strip() not in instance_ids_to_remove]
    payload[key] = filtered
    if key == "entries":
        if "count" in payload:
            payload["count"] = len(filtered)
        if "updated_at" in payload:
            payload["updated_at"] = datetime.now().isoformat(timespec="seconds")
        if "generated_at" in payload:
            payload["generated_at"] = datetime.now().isoformat(timespec="seconds")
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def print_stats() -> None:
    stats = collect_auto_review_stats()
    print("subtype\ttotal\tpositive\tpositive_ratio\tnegative\tuncertain")
    for subtype in sorted(stats):
        total = stats[subtype]["total"]
        positive = stats[subtype]["positive"]
        negative = stats[subtype]["negative"]
        uncertain = stats[subtype]["uncertain"]
        ratio = positive / total if total else 0.0
        print(f"{subtype}\t{total}\t{positive}\t{ratio:.4f}\t{negative}\t{uncertain}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize auto-review labels and optionally delete negative snapshot directories.")
    parser.add_argument("--mode", choices=["stats", "delete"], required=True)
    parser.add_argument("--label", default="negative")
    parser.add_argument("--subtype", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "stats":
        print_stats()
        return

    items = collect_labeled_instances(args.label.strip().lower(), subtype=args.subtype.strip(), limit=args.limit)
    results = delete_snapshot_dirs(items, apply=args.apply)
    if args.apply:
        removed_ids = {item["instance_id"] for item in items}
        update_list_file(SNAPSHOT_PULL_LIST_PATH, "entries", removed_ids)
        update_list_file(SNAPSHOT_CLONE_LIST_PATH, "entries", removed_ids)
    print(json.dumps({"mode": args.mode, "label": args.label, "subtype": args.subtype, "limit": args.limit, "apply": args.apply, "items": results}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
