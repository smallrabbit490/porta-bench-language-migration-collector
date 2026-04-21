#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import mimetypes
import os
import sys
from datetime import datetime
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse, unquote


TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
REVIEW_DIR = DATA_DIR / "review"
PR_METADATA_DIR = DATA_DIR / "raw" / "pr_metadata"
REVIEW_RESULTS_DIR = DATA_DIR / "review_results"
REVIEW_RESULTS_BY_INSTANCE = REVIEW_RESULTS_DIR / "by_instance"
SNAPSHOT_DIR = DATA_DIR / "raw" / "repo_snapshots"

SUPPORTED_SUBTYPES = ["py2_py3", "cpp_python", "java_python", "python_cpp", "python_java"]
SOURCE_CODE_EXTENSIONS = {
    ".py",
    ".pyi",
    ".c",
    ".cc",
    ".cpp",
    ".cxx",
    ".h",
    ".hh",
    ".hpp",
    ".hxx",
    ".java",
    ".md",
    ".txt",
    ".rst",
    ".toml",
    ".yaml",
    ".yml",
    ".ini",
    ".json",
    ".cfg",
    ".cmake",
    ".gradle",
    ".properties",
    ".sh",
    ".bat",
    ".ps1",
    ".xml",
}
MAX_FILE_BYTES = 200_000
MAX_TEXT_CHARS = 32_000

SUBTYPE_ZH = {
    "py2_py3": "Python 2 到 Python 3",
    "cpp_python": "C/C++ 到 Python",
    "java_python": "Java 到 Python",
    "python_cpp": "Python 到 C/C++",
    "python_java": "Python 到 Java",
}

FIELD_GUIDES = {
    "manual_label": "最终收不收这条样本。positive=真迁移，negative=明显噪声，uncertain=还需要二次复核。",
    "implementation_scope": "看迁移的是一个功能模块，还是整个仓库主体。大多数跨语言样本其实都是 partial_feature_migration。",
    "logic_equivalence_scope": "看新旧实现是不是同一逻辑翻译。若 PR 明说 port/rewrite，且功能对应明显，优先 same_logic_translation。",
    "source_target": "确认方向有没有搞反。例如 cpp_python 必须是从 C/C++ 到 Python，不是反过来，也不是单纯绑定。",
    "migration_pattern": "用一句话说清迁移发生了什么，方便后面做汇报和筛选。",
    "reproducible": "你们是否有把握复现实验。没有亲手验证时先写 unknown。",
    "issue_rewrite_ready": "能否进一步改写成 benchmark 任务。信息不够时先写 needs_check。",
    "leakage_risk": "PR 正文是否把解法暴露得太直接。直接点明完整改法通常风险更高。",
    "exclude_reason": "只有当 manual_label=negative 时再填，用英文原因方便后续脚本统计。",
    "notes": "写清楚你为什么这么判断，这是后面复核最有价值的字段。",
}


def ensure_layout() -> None:
    REVIEW_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    REVIEW_RESULTS_BY_INSTANCE.mkdir(parents=True, exist_ok=True)


def load_json(path: Path):
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def read_csv_rows(path: Path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def load_jsonl(path: Path):
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def review_csv_path(subtype: str) -> Path:
    return REVIEW_DIR / f"{subtype}_manual_review.csv"


def metadata_dir(subtype: str) -> Path:
    return PR_METADATA_DIR / subtype


def enriched_index_path(subtype: str) -> Path:
    return metadata_dir(subtype) / "enriched_index.jsonl"


def snapshot_subtype_dir(subtype: str) -> Path:
    return SNAPSHOT_DIR / subtype


def annotation_path(instance_id: str) -> Path:
    return REVIEW_RESULTS_BY_INSTANCE / f"{instance_id}.json"


def index_path() -> Path:
    return REVIEW_RESULTS_DIR / "annotations_index.json"


def safe_rel_path(value: str) -> str:
    return value.replace("\\", "/").lstrip("/")


def is_text_candidate(path: Path) -> bool:
    suffix = path.suffix.lower()
    if suffix in SOURCE_CODE_EXTENSIONS:
        return True
    guessed, _ = mimetypes.guess_type(str(path))
    return bool(guessed and guessed.startswith("text/"))


def read_text_preview(path: Path) -> dict:
    if not path.exists():
        return {"exists": False, "content": "", "truncated": False, "encoding": "utf-8"}
    if path.stat().st_size > MAX_FILE_BYTES:
        return {
            "exists": True,
            "content": f"[文件过大，已跳过预览：{path.stat().st_size} bytes]",
            "truncated": True,
            "encoding": "utf-8",
        }
    if not is_text_candidate(path):
        return {"exists": True, "content": "[该文件类型不适合直接文本预览]", "truncated": False, "encoding": "binary"}
    raw = path.read_text(encoding="utf-8", errors="replace")
    truncated = len(raw) > MAX_TEXT_CHARS
    if truncated:
        raw = raw[:MAX_TEXT_CHARS] + "\n\n[内容过长，已截断]"
    return {"exists": True, "content": raw, "truncated": truncated, "encoding": "utf-8"}


def normalize_bool(value) -> str:
    if isinstance(value, bool):
        return "是" if value else "否"
    value = str(value).strip().lower()
    if value == "true":
        return "是"
    if value == "false":
        return "否"
    return value


def translate_label(value: str) -> str:
    mapping = {"positive": "正例", "negative": "负例", "uncertain": "不确定"}
    return mapping.get((value or "").strip().lower(), "")


def load_saved_annotation(instance_id: str) -> dict:
    path = annotation_path(instance_id)
    if not path.exists():
        return {}
    return load_json(path)


def default_review_row(instance_id: str, subtype: str, enriched: dict, metadata: dict) -> dict:
    repo = metadata.get("repo", {})
    pr_meta = metadata.get("pull_request", {})
    auto_filter = metadata.get("auto_filter", {})
    source_language = ""
    target_language = ""
    source_version = ""
    target_version = ""
    migration_type = "cross_language_migration"
    if subtype == "py2_py3":
        source_language = "python"
        target_language = "python"
        source_version = "2"
        target_version = "3"
        migration_type = "version_migration"
    elif subtype == "cpp_python":
        source_language = "c++"
        target_language = "python"
    elif subtype == "java_python":
        source_language = "java"
        target_language = "python"
    elif subtype == "python_cpp":
        source_language = "python"
        target_language = "c++"
    elif subtype == "python_java":
        source_language = "python"
        target_language = "java"

    return {
        "instance_id": instance_id,
        "scenario": "language_migration",
        "subtype": subtype,
        "subtype_zh": SUBTYPE_ZH.get(subtype, subtype),
        "migration_type": migration_type,
        "migration_type_zh": "",
        "implementation_scope": "",
        "implementation_scope_zh": "",
        "logic_equivalence_scope": "",
        "logic_equivalence_scope_zh": "",
        "repo_full_name": repo.get("full_name", enriched.get("repo_full_name", "")),
        "repo_created_at": repo.get("created_at", enriched.get("repo_created_at", "")),
        "repo_stars": repo.get("stars", enriched.get("repo_stars", "")),
        "pr_number": str(enriched.get("pr_number", "")),
        "pr_url": metadata.get("pr_url", enriched.get("pr_url", "")),
        "pr_created_at": pr_meta.get("created_at", enriched.get("pr_created_at", "")),
        "title": pr_meta.get("title", enriched.get("title", "")),
        "body_summary": pr_meta.get("body", ""),
        "changed_file_summary": "",
        "has_tests_before": str(auto_filter.get("has_tests_before", enriched.get("has_tests_before", ""))).lower(),
        "has_tests_before_zh": normalize_bool(auto_filter.get("has_tests_before", enriched.get("has_tests_before", ""))),
        "adds_new_tests": str(auto_filter.get("adds_new_tests", enriched.get("adds_new_tests", ""))).lower(),
        "adds_new_tests_zh": normalize_bool(auto_filter.get("adds_new_tests", enriched.get("adds_new_tests", ""))),
        "auto_signals": " / ".join(auto_filter.get("auto_signals", enriched.get("auto_signals", [])) or []),
        "auto_signals_zh": " / ".join(auto_filter.get("auto_signals", enriched.get("auto_signals", [])) or []),
        "manual_label": "",
        "manual_label_zh": "",
        "source_language": source_language,
        "source_language_zh": source_language,
        "target_language": target_language,
        "target_language_zh": target_language,
        "source_version": source_version,
        "target_version": target_version,
        "migration_pattern": "",
        "test_framework": "",
        "test_framework_zh": "",
        "build_system": "",
        "build_system_zh": "",
        "reproducible": "",
        "reproducible_zh": "",
        "issue_rewrite_ready": "",
        "issue_rewrite_ready_zh": "",
        "leakage_risk": "",
        "leakage_risk_zh": "",
        "exclude_reason": "",
        "exclude_reason_zh": "",
        "reviewer": "",
        "cross_check_status": "",
        "cross_check_status_zh": "",
        "notes": "",
    }


def sample_snapshot_ready(metadata: dict) -> bool:
    paths = metadata.get("paths", {})
    r0_rel = paths.get("r0_path", "")
    rn_rel = paths.get("rn_path", "")
    if not r0_rel or not rn_rel:
        return False
    return (PROJECT_ROOT / safe_rel_path(r0_rel)).exists() and (PROJECT_ROOT / safe_rel_path(rn_rel)).exists()


def collect_sample_index(subtype: str) -> list[dict]:
    review_rows = {row["instance_id"]: row for row in read_csv_rows(review_csv_path(subtype))}
    enriched_map = {row["instance_id"]: row for row in load_jsonl(enriched_index_path(subtype))}
    items = []
    for metadata_path in sorted(metadata_dir(subtype).glob(f"{subtype}__*.json")):
        instance_id = metadata_path.stem
        metadata = load_json(metadata_path)
        if not sample_snapshot_ready(metadata):
            continue
        saved = load_saved_annotation(instance_id)
        enriched = enriched_map.get(instance_id, {})
        row = review_rows.get(instance_id) or default_review_row(instance_id, subtype, enriched, metadata)
        items.append(
            {
                "instance_id": instance_id,
                "title": row.get("title", "") or metadata.get("pull_request", {}).get("title", ""),
                "repo_full_name": row.get("repo_full_name", ""),
                "pr_number": row.get("pr_number", ""),
                "subtype": subtype,
                "subtype_zh": SUBTYPE_ZH.get(subtype, subtype),
                "repo_stars": row.get("repo_stars", ""),
                "manual_label": saved.get("manual_label") or row.get("manual_label", ""),
                "manual_label_zh": translate_label(saved.get("manual_label") or row.get("manual_label", "")),
                "auto_status": enriched.get("auto_status", ""),
                "auto_signals": enriched.get("auto_signals", []),
            }
        )
    items.sort(key=lambda item: ((item.get("manual_label") or "") == "", item.get("repo_full_name", ""), item.get("instance_id", "")))
    return items


def prioritize_files(changed_files: list[dict]) -> list[dict]:
    def score(item: dict) -> tuple:
        path = item.get("filename", "").lower()
        code_like = any(path.endswith(ext) for ext in [".py", ".cpp", ".cc", ".c", ".h", ".hpp", ".java"])
        tests = "test" in path
        patch = bool(item.get("patch"))
        return (0 if code_like else 1, 0 if patch else 1, 0 if tests else 1, path)

    return sorted(changed_files, key=score)


def sample_detail(instance_id: str) -> dict:
    subtype = instance_id.split("__", 1)[0]
    metadata_path = metadata_dir(subtype) / f"{instance_id}.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata not found for {instance_id}")
    metadata = load_json(metadata_path)
    rows = {row["instance_id"]: row for row in read_csv_rows(review_csv_path(subtype))}
    enriched_rows = {row["instance_id"]: row for row in load_jsonl(enriched_index_path(subtype))}
    row = rows.get(instance_id) or default_review_row(instance_id, subtype, enriched_rows.get(instance_id, {}), metadata)
    saved = load_saved_annotation(instance_id)
    changed_files = prioritize_files(metadata.get("changed_files", []))
    r0_root = PROJECT_ROOT / safe_rel_path(metadata["paths"]["r0_path"])
    rn_root = PROJECT_ROOT / safe_rel_path(metadata["paths"]["rn_path"])

    evidence_files = []
    for item in changed_files[:40]:
        rel = safe_rel_path(item["filename"])
        r0_file = r0_root / rel
        rn_file = rn_root / rel
        evidence_files.append(
            {
                "path": rel,
                "status": item.get("status", ""),
                "changes": item.get("changes", 0),
                "additions": item.get("additions", 0),
                "deletions": item.get("deletions", 0),
                "has_patch": bool(item.get("patch")),
                "patch_preview": item.get("patch", "")[:5000],
                "exists_r0": r0_file.exists(),
                "exists_rn": rn_file.exists(),
            }
        )

    summary = {
        "instance_id": instance_id,
        "subtype": subtype,
        "subtype_zh": SUBTYPE_ZH.get(subtype, subtype),
        "row": row,
        "saved_annotation": saved,
        "metadata": {
            "repo_full_name": metadata["repo"]["full_name"],
            "repo_created_at": metadata["repo"].get("created_at", ""),
            "repo_stars": metadata["repo"].get("stars", ""),
            "license": metadata["repo"].get("license", ""),
            "title": metadata["pull_request"].get("title", ""),
            "body": metadata["pull_request"].get("body", ""),
            "pr_url": metadata["pr_url"],
            "pr_created_at": metadata["pull_request"].get("created_at", ""),
            "merged_at": metadata["pull_request"].get("merged_at", ""),
            "base_sha": metadata.get("base_sha", ""),
            "final_sha": metadata.get("final_sha", ""),
            "matched_queries": metadata.get("matched_queries", []),
            "auto_filter": metadata.get("auto_filter", {}),
            "paths": metadata.get("paths", {}),
        },
        "evidence_files": evidence_files,
        "field_guides": FIELD_GUIDES,
        "recommended_mapping": {
            "left_panel": [
                "先看 PR 标题和正文，判断迁移意图是否明确。",
                "再看改动文件清单，确认是否同时触及源语言和目标语言实现。",
                "随后点开 r0/rn 对照文件，验证是不是同一模块或同一路径在发生替换。",
                "最后参考 patch 片段，看修改是核心逻辑迁移，还是仅包装层/测试/文档变化。",
            ],
            "right_panel": [
                "manual_label 对应“这条证据最后收不收”。",
                "implementation_scope 对应“迁移范围是局部模块还是整仓主体”。",
                "logic_equivalence_scope 对应“左侧看到的新旧代码是不是同一逻辑”。",
                "migration_pattern / notes 对应“把左侧证据翻译成一句清晰的人话”。",
            ],
        },
    }
    return summary


def build_all_patches_text(instance_id: str) -> str:
    subtype = instance_id.split("__", 1)[0]
    metadata = load_json(metadata_dir(subtype) / f"{instance_id}.json")
    chunks = []
    for item in metadata.get("changed_files", []):
        patch = item.get("patch") or ""
        if not patch.strip():
            continue
        chunks.append(f"===== FILE: {item.get('filename', '')} =====\n{patch}")
    if not chunks:
        return "当前样本没有可复制的 patch 内容。"
    return "\n\n".join(chunks)


def update_annotation_index(entry: dict) -> None:
    current = []
    if index_path().exists():
        current = load_json(index_path())
    current = [row for row in current if row.get("instance_id") != entry["instance_id"]]
    current.append(entry)
    current.sort(key=lambda row: row["instance_id"])
    write_json(index_path(), current)


def save_annotation(payload: dict) -> dict:
    ensure_layout()
    instance_id = payload["instance_id"]
    subtype = payload["subtype"]
    now = datetime.now().isoformat(timespec="seconds")
    record = {
        "instance_id": instance_id,
        "subtype": subtype,
        "saved_at": now,
        "manual_label": payload.get("manual_label", ""),
        "implementation_scope": payload.get("implementation_scope", ""),
        "logic_equivalence_scope": payload.get("logic_equivalence_scope", ""),
        "source_language": payload.get("source_language", ""),
        "target_language": payload.get("target_language", ""),
        "source_version": payload.get("source_version", ""),
        "target_version": payload.get("target_version", ""),
        "migration_pattern": payload.get("migration_pattern", ""),
        "test_framework": payload.get("test_framework", ""),
        "build_system": payload.get("build_system", ""),
        "reproducible": payload.get("reproducible", ""),
        "issue_rewrite_ready": payload.get("issue_rewrite_ready", ""),
        "leakage_risk": payload.get("leakage_risk", ""),
        "exclude_reason": payload.get("exclude_reason", ""),
        "reviewer": payload.get("reviewer", ""),
        "cross_check_status": payload.get("cross_check_status", ""),
        "notes": payload.get("notes", ""),
    }
    write_json(annotation_path(instance_id), record)
    update_annotation_index(
        {
            "instance_id": instance_id,
            "subtype": subtype,
            "manual_label": record["manual_label"],
            "reviewer": record["reviewer"],
            "saved_at": now,
        }
    )
    return record


class ReviewHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(TOOL_DIR), **kwargs)

    def log_message(self, format: str, *args) -> None:
        sys.stderr.write("[annotation-tool] " + format % args + "\n")

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def send_json(self, payload, status=HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/subtypes":
            return self.send_json(
                [{"value": subtype, "label": SUBTYPE_ZH[subtype]} for subtype in SUPPORTED_SUBTYPES]
            )
        if parsed.path == "/api/samples":
            subtype = parse_qs(parsed.query).get("subtype", ["cpp_python"])[0]
            return self.send_json(collect_sample_index(subtype))
        if parsed.path.startswith("/api/sample/"):
            instance_id = unquote(parsed.path.split("/api/sample/", 1)[1])
            try:
                return self.send_json(sample_detail(instance_id))
            except FileNotFoundError as exc:
                return self.send_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
        if parsed.path == "/api/file":
            query = parse_qs(parsed.query)
            instance_id = query.get("instance_id", [""])[0]
            side = query.get("side", ["r0"])[0]
            rel_path = query.get("path", [""])[0]
            if not instance_id or side not in {"r0", "rn"} or not rel_path:
                return self.send_json({"error": "instance_id / side / path 缺失"}, status=HTTPStatus.BAD_REQUEST)
            detail = sample_detail(instance_id)
            root_rel = detail["metadata"]["paths"][f"{side}_path"]
            full_path = PROJECT_ROOT / safe_rel_path(root_rel) / safe_rel_path(rel_path)
            return self.send_json(read_text_preview(full_path))
        if parsed.path == "/api/all-patches":
            query = parse_qs(parsed.query)
            instance_id = query.get("instance_id", [""])[0]
            if not instance_id:
                return self.send_json({"error": "instance_id 缺失"}, status=HTTPStatus.BAD_REQUEST)
            try:
                return self.send_json({"content": build_all_patches_text(instance_id)})
            except FileNotFoundError as exc:
                return self.send_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
        if parsed.path == "/api/results":
            if index_path().exists():
                return self.send_json(load_json(index_path()))
            return self.send_json([])
        return super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/save":
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8-sig"))
            saved = save_annotation(payload)
            self.send_json({"ok": True, "saved": saved, "results_dir": str(REVIEW_RESULTS_DIR.relative_to(PROJECT_ROOT))})
        except Exception as exc:  # noqa: BLE001
            self.send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)


def main() -> None:
    ensure_layout()
    port = int(os.environ.get("ANNOTATION_TOOL_PORT", "8765"))
    server = ThreadingHTTPServer(("127.0.0.1", port), ReviewHandler)
    print(f"Annotation tool running at http://127.0.0.1:{port}")
    print(f"Results will be saved under: {REVIEW_RESULTS_DIR}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping annotation tool...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
