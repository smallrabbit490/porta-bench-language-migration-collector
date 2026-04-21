#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import tempfile
import zipfile
from pathlib import Path


ARCHIVE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ARCHIVE_ROOT.parents[2]
PULL_LIST_PATH = ARCHIVE_ROOT / "snapshot_pull_list.json"


def load_manifest(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def collect_manifests() -> list[dict]:
    if PULL_LIST_PATH.exists():
        payload = load_manifest(PULL_LIST_PATH)
        manifests = []
        for item in payload.get("entries", []):
            manifest_path = PROJECT_ROOT / item["manifest_relpath"]
            manifest = load_manifest(manifest_path)
            manifest["_manifest_path"] = manifest_path
            manifests.append(manifest)
        manifests.sort(key=lambda item: item["instance_id"])
        return manifests

    manifests = []
    for manifest_path in ARCHIVE_ROOT.rglob("manifest.json"):
        manifest = load_manifest(manifest_path)
        manifest["_manifest_path"] = manifest_path
        manifests.append(manifest)
    manifests.sort(key=lambda item: item["instance_id"])
    return manifests


def resolve_archive_parts(manifest: dict) -> list[Path]:
    return [PROJECT_ROOT / rel for rel in manifest.get("archive_parts", [])]


def build_extractable_zip(manifest: dict, temp_dir: Path) -> Path:
    parts = resolve_archive_parts(manifest)
    if len(parts) == 1 and parts[0].suffix.lower() == ".zip":
        return parts[0]

    assembled = temp_dir / f"{manifest['instance_id']}.zip"
    with assembled.open("wb") as target:
        for part in parts:
            with part.open("rb") as source:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    target.write(chunk)
    return assembled


def restore_manifest(manifest: dict, force: bool) -> None:
    restore_parent = PROJECT_ROOT / manifest["restore_parent_relpath"]
    target_root = PROJECT_ROOT / manifest["snapshot_root_relpath"]
    if target_root.exists():
        if not force:
            print(f"[skip] {manifest['instance_id']} already exists at {target_root}")
            return
    restore_parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="snapshot_restore_") as temp_dir:
        extractable_zip = build_extractable_zip(manifest, Path(temp_dir))
        with zipfile.ZipFile(extractable_zip, "r") as zip_handle:
            zip_handle.extractall(restore_parent)
    print(f"[ok] restored {manifest['instance_id']} -> {target_root}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restore packaged repo snapshots back into data/raw/repo_snapshots")
    parser.add_argument("--instance-id", default="", help="Restore one snapshot bundle by instance id")
    parser.add_argument("--subtype", default="", help="Restore all bundles for a subtype")
    parser.add_argument("--all", action="store_true", help="Restore every bundled snapshot")
    parser.add_argument("--force", action="store_true", help="Overwrite by extracting again even if target exists")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifests = collect_manifests()
    if not manifests:
        print("No snapshot archive manifests found.")
        return

    selected = manifests
    if args.instance_id:
        selected = [item for item in manifests if item["instance_id"] == args.instance_id]
    elif args.subtype:
        selected = [item for item in manifests if item["subtype"] == args.subtype]
    elif not args.all:
        print("Use --instance-id, --subtype, or --all.")
        return

    if not selected:
        print("No matching manifests found.")
        return

    for manifest in selected:
        restore_manifest(manifest, force=args.force)


if __name__ == "__main__":
    main()
