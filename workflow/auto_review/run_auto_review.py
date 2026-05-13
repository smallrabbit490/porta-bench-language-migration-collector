#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import mimetypes
import os
import queue
import re
import threading
import time
from pathlib import Path
from typing import Any

from openai import OpenAI
from zhipuai import ZhipuAI


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
PR_METADATA_DIR = DATA_DIR / "raw" / "pr_metadata"
AUTO_REVIEW_DIR = DATA_DIR / "auto_review"
CONFIG_PATH = Path(__file__).resolve().parent / "configs" / "pipeline_config.json"
OUTPUT_SCHEMA_PATH = Path(__file__).resolve().parent / "configs" / "output_schema.json"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
ENV_PATH = PROJECT_ROOT / "workflow" / "质朴api使用" / "质朴.env"
REVIEW_RESULTS_DIR = DATA_DIR / "review_results" / "by_instance"
KEY_SLOT_POOL: queue.Queue[str] | None = None
KEY_SLOT_POOL_LOCK = threading.Lock()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def safe_read_text(path: Path, max_chars: int) -> str:
    if not path.exists() or not path.is_file():
        return ""
    guessed, _ = mimetypes.guess_type(str(path))
    if not (
        guessed
        or path.suffix.lower()
        in {".py", ".cpp", ".c", ".h", ".hpp", ".java", ".md", ".txt", ".json", ".toml", ".yaml", ".yml", ".ini", ".cfg", ".xml", ".sh", ".ps1", ".bat", ".rst"}
    ):
        return ""
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return content[:max_chars]


def rank_changed_files(changed_files: list[dict]) -> list[dict]:
    def score(item: dict) -> tuple:
        path = (item.get("filename") or "").lower()
        is_source = path.endswith((".py", ".pyi", ".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx", ".java"))
        is_test = "test" in path or "/tests/" in path or path.startswith("tests/")
        has_patch = bool(item.get("patch"))
        return (0 if is_source else 1, 0 if has_patch else 1, 0 if is_test else 1, path)

    return sorted(changed_files, key=score)


def infer_change_complexity(changed_files: list[dict]) -> dict[str, Any]:
    total_files = len(changed_files)
    code_files = 0
    test_files = 0
    docs_or_config_files = 0
    for item in changed_files:
        path = (item.get("filename") or "").lower()
        if path.endswith((".py", ".pyi", ".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx", ".java")):
            code_files += 1
        if "test" in path or "/tests/" in path or path.startswith("tests/"):
            test_files += 1
        if path.endswith((".md", ".rst", ".txt", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".json")):
            docs_or_config_files += 1

    if total_files <= 2:
        level = "very_low"
    elif total_files <= 5:
        level = "low"
    elif total_files <= 12:
        level = "medium"
    elif total_files <= 25:
        level = "high"
    else:
        level = "very_high"

    return {
        "level": level,
        "total_changed_files": total_files,
        "code_file_count": code_files,
        "test_file_count": test_files,
        "docs_or_config_file_count": docs_or_config_files,
    }


def find_metadata_path(instance_id: str) -> Path:
    subtype = instance_id.split("__", 1)[0]
    return PR_METADATA_DIR / subtype / f"{instance_id}.json"


def list_instance_ids_for_subtype(subtype: str) -> list[str]:
    metadata_root = PR_METADATA_DIR / subtype
    instance_ids: list[str] = []
    for path in sorted(metadata_root.glob(f"{subtype}__*.json")):
        instance_ids.append(path.stem)
    return instance_ids


def metadata_has_snapshots(instance_id: str) -> bool:
    metadata_path = find_metadata_path(instance_id)
    if not metadata_path.exists():
        return False
    metadata = load_json(metadata_path)
    paths = metadata.get("paths", {})
    r0_rel = str(paths.get("r0_path", "")).replace("\\", "/")
    rn_rel = str(paths.get("rn_path", "")).replace("\\", "/")
    if not r0_rel or not rn_rel:
        return False
    return (PROJECT_ROOT / r0_rel).exists() and (PROJECT_ROOT / rn_rel).exists()


def evidence_path(instance_id: str) -> Path:
    subtype = instance_id.split("__", 1)[0]
    return AUTO_REVIEW_DIR / "evidence" / subtype / f"{instance_id}.json"


def draft_path(instance_id: str, suffix: str) -> Path:
    subtype = instance_id.split("__", 1)[0]
    return AUTO_REVIEW_DIR / "drafts" / subtype / f"{instance_id}.{suffix}.json"


def final_path(instance_id: str) -> Path:
    return AUTO_REVIEW_DIR / "final" / "by_instance" / f"{instance_id}.json"


def trace_path(instance_id: str) -> Path:
    return AUTO_REVIEW_DIR / "final" / "traces" / f"{instance_id}.json"


def load_env_file(path: Path) -> dict[str, str]:
    env_map: dict[str, str] = {}
    if not path.exists():
        return env_map
    for raw_line in read_text(path).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        cleaned = value.strip()
        if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
            cleaned = cleaned[1:-1]
        env_map[key.strip()] = cleaned
    return env_map


def resolve_env_path(config: dict[str, Any]) -> Path:
    configured = str(config.get("env_file", "")).strip()
    if configured:
        path = Path(configured)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        return path
    return PROJECT_ROOT / ".env"


def load_runtime_env_map(config: dict[str, Any]) -> dict[str, str]:
    env_map = load_env_file(resolve_env_path(config))
    merged = dict(env_map)
    for key, value in os.environ.items():
        if value:
            merged[key] = value
    return merged


def resolve_model_provider(config: dict[str, Any]) -> str:
    return str(config.get("model_provider", "glm")).strip().lower()


def resolve_model_name(config: dict[str, Any], env_map: dict[str, str] | None = None) -> str:
    runtime_env = env_map or load_runtime_env_map(config)
    env_name = str(config.get("model_env_name", "")).strip()
    if env_name:
        env_value = runtime_env.get(env_name, "").strip()
        if env_value:
            return env_value
    return str(config.get("model_name", "glm-4.7")).strip()


def resolve_base_url(config: dict[str, Any], env_map: dict[str, str] | None = None) -> str:
    runtime_env = env_map or load_runtime_env_map(config)
    env_name = str(config.get("base_url_env_name", "")).strip()
    if env_name:
        env_value = runtime_env.get(env_name, "").strip()
        if env_value:
            return env_value
    return str(config.get("base_url", "")).strip()


def get_api_keys(config: dict[str, Any]) -> list[str]:
    env_map = load_runtime_env_map(config)
    keys: list[str] = []
    for key_name in config.get("api_key_priority", []):
        value = env_map.get(key_name, "").strip()
        if value:
            keys.append(value)
    unique_keys = list(dict.fromkeys(keys))
    if unique_keys:
        return unique_keys
    raise RuntimeError(f"No API key found in {resolve_env_path(config)}")


def acquire_key_slot(config: dict[str, Any]) -> str:
    global KEY_SLOT_POOL
    with KEY_SLOT_POOL_LOCK:
        if KEY_SLOT_POOL is None:
            keys = get_api_keys(config)
            per_key_limit = max(1, int(config.get("max_concurrency_per_key", 5)))
            KEY_SLOT_POOL = queue.Queue()
            for key in keys:
                for _ in range(per_key_limit):
                    KEY_SLOT_POOL.put(key)
    assert KEY_SLOT_POOL is not None
    return KEY_SLOT_POOL.get()


def release_key_slot(api_key: str) -> None:
    if KEY_SLOT_POOL is not None:
        KEY_SLOT_POOL.put(api_key)


def create_llm_client(config: dict[str, Any], api_key: str):
    timeout_sec = int(config.get("llm_timeout_sec", 60))
    max_retries = int(config.get("llm_max_retries", 0))
    provider = resolve_model_provider(config)
    if provider in {"openai", "openai_compatible"}:
        kwargs: dict[str, Any] = {
            "api_key": api_key,
            "timeout": timeout_sec,
            "max_retries": max_retries,
        }
        base_url = resolve_base_url(config)
        if base_url:
            kwargs["base_url"] = base_url
        return OpenAI(**kwargs)
    return ZhipuAI(api_key=api_key, timeout=timeout_sec, max_retries=max_retries)


def create_chat_completion(
    client: Any,
    config: dict[str, Any],
    messages: list[dict[str, Any]],
    timeout_sec: int,
    max_tokens: int,
    temperature: float,
    top_p: float,
):
    kwargs: dict[str, Any] = {
        "model": resolve_model_name(config),
        "messages": messages,
        "temperature": temperature,
        "top_p": top_p,
        "max_tokens": max_tokens,
        "timeout": timeout_sec,
    }
    if resolve_model_provider(config) in {"openai", "openai_compatible"} and config.get("openai_disable_thinking", True):
        kwargs["extra_body"] = {"thinking": {"type": "disabled"}}
    return client.chat.completions.create(**kwargs)


def build_evidence(instance_id: str) -> Path:
    config = load_json(CONFIG_PATH)
    metadata_path = find_metadata_path(instance_id)
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata not found: {metadata_path}")

    metadata = load_json(metadata_path)
    subtype = metadata["subtype"]
    paths = metadata.get("paths", {})
    r0_root = PROJECT_ROOT / str(paths.get("r0_path", "")).replace("\\", "/")
    rn_root = PROJECT_ROOT / str(paths.get("rn_path", "")).replace("\\", "/")
    changed_files = rank_changed_files(metadata.get("changed_files", []))

    max_ranked_files = int(config.get("max_ranked_files", 12))
    max_patch_chars = int(config.get("max_patch_chars_per_file", 4000))
    max_preview_chars = int(config.get("max_file_preview_chars", 5000))

    ranked = []
    for item in changed_files[:max_ranked_files]:
        rel = (item.get("filename") or "").replace("\\", "/")
        ranked.append(
            {
                "path": rel,
                "status": item.get("status", ""),
                "additions": item.get("additions", 0),
                "deletions": item.get("deletions", 0),
                "changes": item.get("changes", 0),
                "patch_preview": (item.get("patch") or "")[:max_patch_chars],
                "r0_exists": (r0_root / rel).exists(),
                "rn_exists": (rn_root / rel).exists(),
                "r0_preview": safe_read_text(r0_root / rel, max_preview_chars),
                "rn_preview": safe_read_text(rn_root / rel, max_preview_chars),
            }
        )

    repo = metadata.get("repo", {})
    pull_request = metadata.get("pull_request", {})
    auto_filter = metadata.get("auto_filter", {})
    change_complexity = infer_change_complexity(metadata.get("changed_files", []))
    evidence = {
        "instance_id": instance_id,
        "subtype": subtype,
        "repo": {
            "full_name": repo.get("full_name", ""),
            "created_at": repo.get("created_at", ""),
            "stars": repo.get("stars", ""),
            "license": repo.get("license", ""),
        },
        "pull_request": {
            "title": pull_request.get("title", ""),
            "body": pull_request.get("body", ""),
            "created_at": pull_request.get("created_at", ""),
            "merged_at": pull_request.get("merged_at", ""),
            "labels": pull_request.get("labels", []),
            "pr_url": metadata.get("pr_url", ""),
        },
        "matched_queries": metadata.get("matched_queries", []),
        "base_sha": metadata.get("base_sha", ""),
        "final_sha": metadata.get("final_sha", ""),
        "change_complexity": change_complexity,
        "auto_filter": {
            "status": auto_filter.get("status", ""),
            "exclude_reasons": auto_filter.get("exclude_reasons", []),
            "auto_signals": auto_filter.get("auto_signals", []),
            "has_tests_before": auto_filter.get("has_tests_before", False),
            "adds_new_tests": auto_filter.get("adds_new_tests", False),
            "touches_tests": auto_filter.get("touches_tests", False),
            "touches_code": auto_filter.get("touches_code", False),
            "touches_source_language": auto_filter.get("touches_source_language", False),
            "touches_target_language": auto_filter.get("touches_target_language", False),
            "cross_language_mapping_visible": auto_filter.get("cross_language_mapping_visible", False),
        },
        "ranked_files": ranked,
    }

    output_path = evidence_path(instance_id)
    dump_json(output_path, evidence)
    return output_path


def infer_direction(subtype: str) -> dict[str, str]:
    mapping = {
        "py2_py3": {"source_language": "python", "target_language": "python", "source_version": "2", "target_version": "3", "migration_type": "version_migration"},
        "cpp_python": {"source_language": "c++", "target_language": "python", "source_version": "", "target_version": "", "migration_type": "cross_language_migration"},
        "java_python": {"source_language": "java", "target_language": "python", "source_version": "", "target_version": "", "migration_type": "cross_language_migration"},
        "python_cpp": {"source_language": "python", "target_language": "c++", "source_version": "", "target_version": "", "migration_type": "cross_language_migration"},
        "python_java": {"source_language": "python", "target_language": "java", "source_version": "", "target_version": "", "migration_type": "cross_language_migration"},
    }
    return mapping[subtype]


def infer_test_framework(evidence: dict[str, Any]) -> str:
    corpus = "\n".join(item.get("path", "") for item in evidence.get("ranked_files", []))
    text = f"{evidence.get('pull_request', {}).get('body', '')}\n{corpus}".lower()
    if "pytest" in text:
        return "pytest"
    if "unittest" in text:
        return "unittest"
    if "nose" in text:
        return "nose"
    if "doctest" in text:
        return "doctest"
    if "test" in text:
        return "test_files_detected"
    return ""


def infer_build_system(evidence: dict[str, Any]) -> str:
    file_paths = [item.get("path", "").lower() for item in evidence.get("ranked_files", [])]
    if any(path.endswith("pyproject.toml") for path in file_paths):
        body = evidence.get("pull_request", {}).get("body", "").lower()
        if "poetry" in body:
            return "poetry"
        return "poetry"
    if any(path.endswith("setup.py") or path.endswith("setup.cfg") for path in file_paths):
        return "setuptools"
    if any(path.endswith("tox.ini") for path in file_paths):
        return "tox"
    if any(path.endswith("makefile") for path in file_paths):
        return "make"
    if any(path.endswith("cmakelists.txt") or "/cmake/" in path for path in file_paths):
        return "cmake"
    if any(path.endswith(".gradle") or path.endswith("pom.xml") for path in file_paths):
        return "gradle"
    return ""


def infer_implementation_scope(evidence: dict[str, Any]) -> str:
    subtype = evidence["subtype"]
    if subtype == "py2_py3":
        return "not_applicable"
    body = (evidence.get("pull_request", {}).get("body", "") or "").lower()
    title = (evidence.get("pull_request", {}).get("title", "") or "").lower()
    ranked_files = evidence.get("ranked_files", [])
    if any(phrase in f"{title}\n{body}" for phrase in ("full migration", "entire repository", "whole repo", "rewrite the whole", "remove all java", "remove all c++")):
        return "full_repo_translation"
    if len(ranked_files) >= 10 and sum(1 for item in ranked_files if item.get("r0_exists") and item.get("rn_exists")) >= 8:
        return "partial_feature_migration"
    return "partial_feature_migration"


def infer_logic_scope(evidence: dict[str, Any]) -> str:
    signals = set(evidence.get("auto_filter", {}).get("auto_signals", []))
    text = f"{evidence.get('pull_request', {}).get('title', '')}\n{evidence.get('pull_request', {}).get('body', '')}".lower()
    if "strict_migration_signal" in signals or "rewrite" in text or "port to" in text or "migrate" in text:
        return "same_logic_translation"
    if evidence.get("auto_filter", {}).get("cross_language_mapping_visible"):
        return "partial_logic_replacement"
    return "unclear_logic_mapping"


def infer_manual_label(evidence: dict[str, Any]) -> tuple[str, str]:
    auto_filter = evidence.get("auto_filter", {})
    exclude_reasons = auto_filter.get("exclude_reasons", [])
    signals = set(auto_filter.get("auto_signals", []))
    text = f"{evidence.get('pull_request', {}).get('title', '')}\n{evidence.get('pull_request', {}).get('body', '')}".lower()
    if exclude_reasons:
        return "negative", exclude_reasons[0]
    if any(noise in signals for noise in {"binding_wrapper_noise", "python_cpp_wrapper_noise", "java_python_bridge_noise", "python_java_bridge_noise"}):
        return "negative", next(
            (noise for noise in ["binding_wrapper_noise", "python_cpp_wrapper_noise", "java_python_bridge_noise", "python_java_bridge_noise"] if noise in signals),
            "",
        )
    if "strict_migration_signal" in signals or "keyword_match" in signals:
        return "positive", ""
    if "rewrite" in text or "port to" in text or "remove python 2" in text or "python 3" in text:
        return "positive", ""
    return "uncertain", ""


def infer_leakage_risk(evidence: dict[str, Any]) -> str:
    body = evidence.get("pull_request", {}).get("body", "") or ""
    body_lower = body.lower()
    if any(token in body_lower for token in ("all tests passed", "replace the previous", "new helper", "update _reclaim_port_processes", "fixes by", "testing")):
        return "high"
    if len(body) > 500:
        return "medium"
    return "low"


def heuristic_review(instance_id: str) -> Path:
    evidence = load_json(evidence_path(instance_id))
    direction = infer_direction(evidence["subtype"])
    manual_label, exclude_reason = infer_manual_label(evidence)
    result = {
        "instance_id": instance_id,
        "subtype": evidence["subtype"],
        "manual_label": manual_label,
        "implementation_scope": infer_implementation_scope(evidence),
        "logic_equivalence_scope": infer_logic_scope(evidence),
        "source_language": direction["source_language"],
        "target_language": direction["target_language"],
        "source_version": direction["source_version"],
        "target_version": direction["target_version"],
        "migration_pattern": "",
        "test_framework": infer_test_framework(evidence),
        "build_system": infer_build_system(evidence),
        "reproducible": "unknown",
        "issue_rewrite_ready": "needs_check",
        "leakage_risk": infer_leakage_risk(evidence),
        "exclude_reason": exclude_reason if manual_label == "negative" else "",
        "reviewer": "auto_heuristic",
        "cross_check_status": "pending",
        "notes": "",
        "migration_type": direction["migration_type"],
        "change_complexity_level": evidence.get("change_complexity", {}).get("level", ""),
    }
    output_path = draft_path(instance_id, "heuristic")
    dump_json(output_path, result)
    return output_path


def extract_json_block(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", stripped, re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def repair_json_with_llm(
    client: Any,
    config: dict[str, Any],
    broken_text: str,
    timeout_sec: int,
    max_tokens: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    repair_messages = [
        {
            "role": "system",
            "content": "Convert the user's broken JSON-like text into one valid JSON object. Output JSON only.",
        },
        {
            "role": "user",
            "content": broken_text,
        },
    ]
    response = create_chat_completion(
        client,
        config,
        repair_messages,
        timeout_sec,
        max_tokens,
        0.0,
        0.1,
    )
    repaired_content = response.choices[0].message.content
    return extract_json_block(repaired_content), {
        "messages": repair_messages,
        "raw_content": repaired_content,
        "model": resolve_model_name(config),
    }


def build_llm_request(evidence: dict[str, Any], heuristic: dict[str, Any]) -> dict[str, Any]:
    config = load_json(CONFIG_PATH)
    system_prompt = read_text(PROMPTS_DIR / "review_system_prompt.md")
    user_prompt = read_text(PROMPTS_DIR / "review_user_prompt.md")
    llm_ranked_files = int(config.get("llm_ranked_files", 5))
    llm_patch_chars = int(config.get("llm_patch_chars_per_file", 1200))
    llm_preview_chars = int(config.get("llm_file_preview_chars", 1200))
    ranked_files = []
    for item in evidence.get("ranked_files", [])[:llm_ranked_files]:
        ranked_files.append(
            {
                "path": item.get("path", ""),
                "status": item.get("status", ""),
                "additions": item.get("additions", 0),
                "deletions": item.get("deletions", 0),
                "changes": item.get("changes", 0),
                "patch_preview": (item.get("patch_preview") or "")[:llm_patch_chars],
                "r0_exists": item.get("r0_exists", False),
                "rn_exists": item.get("rn_exists", False),
                "r0_preview": (item.get("r0_preview") or "")[:llm_preview_chars],
                "rn_preview": (item.get("rn_preview") or "")[:llm_preview_chars],
            }
        )
    payload = {
        "task": "Fill the review fields for this sample.",
        "heuristic_prefill": heuristic,
        "evidence": {
            "instance_id": evidence.get("instance_id", ""),
            "subtype": evidence.get("subtype", ""),
            "repo": evidence.get("repo", {}),
            "pull_request": evidence.get("pull_request", {}),
            "matched_queries": evidence.get("matched_queries", []),
            "base_sha": evidence.get("base_sha", ""),
            "final_sha": evidence.get("final_sha", ""),
            "change_complexity": evidence.get("change_complexity", {}),
            "auto_filter": evidence.get("auto_filter", {}),
            "ranked_files": ranked_files,
        },
    }
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt + "\n\nEvidence JSON:\n" + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))},
    ]
    return {
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "evidence_payload": payload,
        "messages": messages,
    }


def llm_review(instance_id: str, api_key: str | None = None) -> Path:
    config = load_json(CONFIG_PATH)
    evidence = load_json(evidence_path(instance_id))
    heuristic = load_json(draft_path(instance_id, "heuristic"))
    active_api_key = api_key or get_api_keys(config)[0]
    timeout_sec = int(config.get("llm_timeout_sec", 60))
    max_tokens = int(config.get("llm_max_tokens", 1200))
    client = create_llm_client(config, active_api_key)
    request_bundle = build_llm_request(evidence, heuristic)
    messages = request_bundle["messages"]
    response = create_chat_completion(
        client,
        config,
        messages,
        timeout_sec,
        max_tokens,
        0.1,
        0.7,
    )
    content = response.choices[0].message.content
    repair_trace: dict[str, Any] = {}
    try:
        parsed = extract_json_block(content)
    except json.JSONDecodeError:
        parsed, repair_trace = repair_json_with_llm(
            client,
            config,
            content,
            timeout_sec,
            max_tokens,
        )
    output = {
        "instance_id": instance_id,
        "subtype": evidence["subtype"],
        "model": resolve_model_name(config),
        "model_provider": resolve_model_provider(config),
        "api_key_alias": "secondary" if api_key and len(get_api_keys(config)) > 1 and api_key == get_api_keys(config)[1] else "primary",
        "request": {
            "system_prompt_path": str((PROMPTS_DIR / "review_system_prompt.md").relative_to(PROJECT_ROOT)),
            "user_prompt_path": str((PROMPTS_DIR / "review_user_prompt.md").relative_to(PROJECT_ROOT)),
            "system_prompt": request_bundle["system_prompt"],
            "user_prompt": request_bundle["user_prompt"],
            "evidence_payload": request_bundle["evidence_payload"],
            "messages": messages,
            "timeout_sec": timeout_sec,
            "max_tokens": max_tokens,
            "temperature": 0.1,
            "top_p": 0.7,
            "base_url": resolve_base_url(config),
            "thinking_disabled": bool(config.get("openai_disable_thinking", True)),
        },
        "raw_content": content,
        "repair_trace": repair_trace,
        "parsed": parsed,
    }
    output_path = draft_path(instance_id, "llm")
    dump_json(output_path, output)
    return output_path


def validate_allowed_values(payload: dict[str, Any]) -> dict[str, Any]:
    schema = load_json(OUTPUT_SCHEMA_PATH)
    for field, allowed_values in schema.get("allowed_values", {}).items():
        value = payload.get(field, "")
        if value and value not in allowed_values:
            payload[field] = ""
    return payload


def find_invalid_review_fields(payload: dict[str, Any]) -> list[str]:
    schema = load_json(OUTPUT_SCHEMA_PATH)
    invalid: list[str] = []
    for field in schema["required_review_fields"]:
        if field in {"exclude_reason", "source_version", "target_version", "migration_pattern", "test_framework", "build_system"}:
            continue
        value = payload.get(field, "")
        if value in (None, ""):
            invalid.append(field)
    for field, allowed_values in schema.get("allowed_values", {}).items():
        value = payload.get(field, "")
        if value and value not in allowed_values:
            invalid.append(field)
    if payload.get("manual_label") == "negative" and not payload.get("exclude_reason"):
        invalid.append("exclude_reason")
    if payload.get("manual_label") != "negative" and payload.get("exclude_reason"):
        invalid.append("exclude_reason")
    if payload.get("subtype") == "py2_py3":
        expected = {
            "source_language": "python",
            "target_language": "python",
            "source_version": "2",
            "target_version": "3",
            "implementation_scope": "not_applicable",
        }
        for field, expected_value in expected.items():
            if payload.get(field) != expected_value:
                invalid.append(field)
    notes = str(payload.get("notes", "") or "").strip()
    if len(notes) < 20:
        invalid.append("notes")
    return sorted(set(invalid))


def force_direction_fields(merged: dict[str, Any]) -> None:
    direction = infer_direction(merged["subtype"])
    merged["source_language"] = direction["source_language"]
    merged["target_language"] = direction["target_language"]
    merged["source_version"] = direction["source_version"]
    merged["target_version"] = direction["target_version"]


def self_check(instance_id: str) -> Path:
    heuristic = load_json(draft_path(instance_id, "heuristic"))
    llm = load_json(draft_path(instance_id, "llm"))
    parsed = llm.get("parsed", {})
    merged = dict(heuristic)
    for key, value in parsed.items():
        if value not in (None, ""):
            merged[key] = value

    merged["reviewer"] = merged.get("reviewer") or "auto_glm47"
    merged["cross_check_status"] = merged.get("cross_check_status") or "pending"
    force_direction_fields(merged)

    if merged["subtype"] == "py2_py3":
        merged["implementation_scope"] = "not_applicable"
    elif merged.get("manual_label") != "positive":
        merged["implementation_scope"] = "not_applicable"
        if not merged.get("logic_equivalence_scope"):
            merged["logic_equivalence_scope"] = "unclear_logic_mapping"

    if merged.get("manual_label") == "negative" and not merged.get("exclude_reason"):
        merged["exclude_reason"] = heuristic.get("exclude_reason") or "needs_manual_check"
    if merged.get("manual_label") != "negative":
        merged["exclude_reason"] = ""

    schema = load_json(OUTPUT_SCHEMA_PATH)
    for field, allowed_values in schema.get("allowed_values", {}).items():
        value = merged.get(field, "")
        if value and value not in allowed_values:
            merged[field] = heuristic.get(field, "") if heuristic.get(field, "") in allowed_values else ""

    if not merged.get("logic_equivalence_scope"):
        merged["logic_equivalence_scope"] = heuristic.get("logic_equivalence_scope", "unclear_logic_mapping")
    if not merged.get("issue_rewrite_ready"):
        merged["issue_rewrite_ready"] = heuristic.get("issue_rewrite_ready", "needs_check")
    if not merged.get("leakage_risk"):
        merged["leakage_risk"] = heuristic.get("leakage_risk", "medium")

    if not merged.get("notes"):
        merged["notes"] = (
            f"Auto review draft based on PR title/body, changed files, patch previews, "
            f"and r0/rn code comparison for subtype {merged['subtype']}."
        )

    merged = validate_allowed_values(merged)
    invalid_fields = find_invalid_review_fields(merged)
    if invalid_fields:
        raise RuntimeError(f"Invalid checked review fields: {', '.join(invalid_fields)}")
    output_path = draft_path(instance_id, "checked")
    dump_json(output_path, merged)
    return output_path


def merge_results(instance_id: str, write_review_results: bool = False) -> Path:
    checked = load_json(draft_path(instance_id, "checked"))
    schema = load_json(OUTPUT_SCHEMA_PATH)
    final_payload = {field: checked.get(field, "") for field in schema["required_review_fields"]}
    final_payload["instance_id"] = instance_id
    final_payload["subtype"] = checked["subtype"]
    dump_json(final_path(instance_id), final_payload)
    dump_json(
        trace_path(instance_id),
        {
            "instance_id": instance_id,
            "generated_from": {
                "evidence": str(evidence_path(instance_id).relative_to(PROJECT_ROOT)),
                "heuristic": str(draft_path(instance_id, "heuristic").relative_to(PROJECT_ROOT)),
                "llm": str(draft_path(instance_id, "llm").relative_to(PROJECT_ROOT)),
                "checked": str(draft_path(instance_id, "checked").relative_to(PROJECT_ROOT)),
            },
            "final_review_path": str(final_path(instance_id).relative_to(PROJECT_ROOT)),
        },
    )
    if write_review_results:
        dump_json(REVIEW_RESULTS_DIR / f"{instance_id}.json", final_payload)
    return final_path(instance_id)


def run_all_for_instance(instance_id: str, write_review_results: bool = False) -> Path:
    build_evidence(instance_id)
    heuristic_review(instance_id)
    llm_review(instance_id)
    self_check(instance_id)
    return merge_results(instance_id, write_review_results=write_review_results)


def run_all_for_instance_with_retry(instance_id: str, attempts: int, write_review_results: bool = False) -> tuple[Path, int]:
    attempts = max(1, attempts)
    last_error: Exception | None = None
    config = load_json(CONFIG_PATH)
    for attempt in range(1, attempts + 1):
        api_key = acquire_key_slot(config)
        try:
            build_evidence(instance_id)
            heuristic_review(instance_id)
            llm_review(instance_id, api_key=api_key)
            self_check(instance_id)
            return merge_results(instance_id, write_review_results=write_review_results), attempt
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < attempts:
                time.sleep(min(2 * attempt, 10))
        finally:
            release_key_slot(api_key)
    if last_error is None:
        raise RuntimeError("Unknown auto-review failure")
    raise last_error


def format_seconds(seconds: float) -> str:
    total = max(0, int(round(seconds)))
    minutes, sec = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def run_one_batch_item(instance_id: str, attempts: int, write_review_results: bool, resume: bool) -> dict[str, Any]:
    item_started = time.time()
    if resume and final_path(instance_id).exists():
        return {
            "instance_id": instance_id,
            "status": "skipped",
            "attempts": 0,
            "elapsed_sec": round(time.time() - item_started, 2),
            "output_path": str(final_path(instance_id)),
            "error": "",
        }

    try:
        output_path, used_attempts = run_all_for_instance_with_retry(
            instance_id,
            attempts=attempts,
            write_review_results=write_review_results,
        )
        status = "ok"
        error_message = ""
    except Exception as exc:  # noqa: BLE001
        output_path = None
        used_attempts = attempts
        status = "error"
        error_message = f"{type(exc).__name__}: {exc}"

    return {
        "instance_id": instance_id,
        "status": status,
        "attempts": used_attempts,
        "elapsed_sec": round(time.time() - item_started, 2),
        "output_path": str(output_path) if output_path else "",
        "error": error_message,
    }


def run_batch(
    subtype: str,
    limit: int,
    write_review_results: bool = False,
    workers: int = 1,
    attempts: int = 1,
    resume: bool = False,
    reviewer_filter: str = "",
) -> dict[str, Any]:
    candidates = [instance_id for instance_id in list_instance_ids_for_subtype(subtype) if metadata_has_snapshots(instance_id)]
    if reviewer_filter:
        reviewer_filter_normalized = reviewer_filter.strip().lower()
        reviewer_candidates: list[str] = []
        for review_path in sorted(REVIEW_RESULTS_DIR.glob(f"{subtype}__*.json")):
            review_payload = load_json(review_path)
            reviewer = str(review_payload.get("reviewer", "")).strip().lower()
            if reviewer == reviewer_filter_normalized:
                reviewer_candidates.append(review_path.stem)
        allowed = set(reviewer_candidates)
        candidates = [instance_id for instance_id in candidates if instance_id in allowed]
    if limit > 0:
        candidates = candidates[:limit]
    if not candidates:
        raise RuntimeError(f"No snapshot-ready instances found for subtype={subtype}")

    results: list[dict[str, Any]] = []
    started_at = time.time()
    total = len(candidates)
    workers = max(1, min(workers, total))
    attempts = max(1, attempts)
    print(
        f"Starting auto-review batch subtype={subtype} total={total} "
        f"workers={workers} attempts={attempts} resume={resume} reviewer_filter={reviewer_filter or 'none'}",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_instance = {
            executor.submit(run_one_batch_item, instance_id, attempts, write_review_results, resume): instance_id
            for instance_id in candidates
        }
        for index, future in enumerate(as_completed(future_to_instance), start=1):
            instance_id = future_to_instance[future]
            try:
                item = future.result()
            except Exception as exc:  # noqa: BLE001
                item = {
                    "instance_id": instance_id,
                    "status": "error",
                    "attempts": attempts,
                    "elapsed_sec": 0,
                    "output_path": "",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            results.append(item)
            avg_elapsed = sum(result["elapsed_sec"] for result in results) / len(results)
            remaining = max(total - index, 0)
            eta = remaining * avg_elapsed / workers
            print(
                f"[{index}/{total}] {item['instance_id']} status={item['status']} "
                f"attempts={item.get('attempts', '')} elapsed={format_seconds(item['elapsed_sec'])} "
                f"avg={format_seconds(avg_elapsed)} eta={format_seconds(eta)}",
                flush=True,
            )
            if item["error"]:
                print(f"  error: {item['error']}", flush=True)
            summary_path = AUTO_REVIEW_DIR / "final" / "batch_runs" / f"{subtype}__limit{total}.json"
            dump_json(
                summary_path,
                {
                    "subtype": subtype,
                    "requested_limit": limit,
                    "processed": len(results),
                    "success": sum(1 for result in results if result["status"] == "ok"),
                    "skipped": sum(1 for result in results if result["status"] == "skipped"),
                    "error": sum(1 for result in results if result["status"] == "error"),
                    "workers": workers,
                    "attempts": attempts,
                    "resume": resume,
                    "reviewer_filter": reviewer_filter,
                    "partial": len(results) < total,
                    "results": sorted(results, key=lambda row: row["instance_id"]),
                },
            )

    total_elapsed = time.time() - started_at
    summary = {
        "subtype": subtype,
        "requested_limit": limit,
        "processed": len(results),
        "success": sum(1 for item in results if item["status"] == "ok"),
        "skipped": sum(1 for item in results if item["status"] == "skipped"),
        "error": sum(1 for item in results if item["status"] == "error"),
        "workers": workers,
        "attempts": attempts,
        "resume": resume,
        "reviewer_filter": reviewer_filter,
        "avg_elapsed_sec": round(sum(item["elapsed_sec"] for item in results) / len(results), 2),
        "total_elapsed_sec": round(total_elapsed, 2),
        "partial": False,
        "results": sorted(results, key=lambda row: row["instance_id"]),
    }
    summary_path = AUTO_REVIEW_DIR / "final" / "batch_runs" / f"{subtype}__limit{len(results)}.json"
    dump_json(summary_path, summary)
    print(f"Batch summary written to: {summary_path}", flush=True)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto review workflow scaffold for snapshot-based annotation")
    parser.add_argument(
        "--stage",
        choices=["build-evidence", "heuristic-review", "llm-review", "self-check", "merge-results", "run-all", "run-batch"],
        required=True,
    )
    parser.add_argument("--instance-id", default="")
    parser.add_argument("--subtype", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--reviewer-filter", default="")
    parser.add_argument("--write-review-results", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.stage == "run-batch":
        if not args.subtype:
            raise RuntimeError("--subtype is required for run-batch")
        run_batch(
            args.subtype,
            args.limit,
            write_review_results=args.write_review_results,
            workers=args.workers,
            attempts=args.attempts,
            resume=args.resume,
            reviewer_filter=args.reviewer_filter,
        )
        return
    if not args.instance_id:
        raise RuntimeError("--instance-id is required for non-batch stages")
    if args.stage == "build-evidence":
        output_path = build_evidence(args.instance_id)
        print(f"Evidence written to: {output_path}")
        return
    if args.stage == "heuristic-review":
        output_path = heuristic_review(args.instance_id)
        print(f"Heuristic draft written to: {output_path}")
        return
    if args.stage == "llm-review":
        output_path = llm_review(args.instance_id)
        print(f"LLM draft written to: {output_path}")
        return
    if args.stage == "self-check":
        output_path = self_check(args.instance_id)
        print(f"Checked draft written to: {output_path}")
        return
    if args.stage == "merge-results":
        output_path = merge_results(args.instance_id, write_review_results=args.write_review_results)
        print(f"Final review written to: {output_path}")
        return
    if args.stage == "run-all":
        output_path, _ = run_all_for_instance_with_retry(
            args.instance_id,
            attempts=args.attempts,
            write_review_results=args.write_review_results,
        )
        print(f"Auto review pipeline finished: {output_path}")
        return
    raise NotImplementedError(args.stage)


if __name__ == "__main__":
    main()
