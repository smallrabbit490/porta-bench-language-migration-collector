#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Porta-Bench language-migration data collector.

Stages:
1. collect       - search GitHub PRs and keep unique PR candidates
2. enrich        - fetch PR metadata, apply auto filters, and save r0/rn snapshots
3. export-review - export CSV for manual review
4. apply-review  - convert reviewed CSV into processed candidates
5. package       - build subtype stats and the final jsonl dataset
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import random
import re
import shutil
import socket
import subprocess
import tempfile
import textwrap
import time
from urllib.parse import urlparse
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests
from requests.adapters import HTTPAdapter
import urllib3
from urllib3.util.retry import Retry


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SCENARIO = "language_migration"
SUPPORTED_SUBTYPES = ("py2_py3", "cpp_python", "java_python", "python_cpp", "python_java")
SUPPORTED_STAGES = ("collect", "enrich", "export-review", "apply-review", "package")

CONFIG_DIR = PROJECT_ROOT / "configs"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SEARCH_RESULTS_DIR = RAW_DIR / "search_results"
PR_METADATA_DIR = RAW_DIR / "pr_metadata"
SNAPSHOT_DIR = RAW_DIR / "repo_snapshots"
REVIEW_DIR = DATA_DIR / "review"
PROCESSED_DIR = DATA_DIR / "processed"
STATS_DIR = DATA_DIR / "stats"
LOG_DIR = PROJECT_ROOT / "logs"
TMP_DIR = PROJECT_ROOT / "tmp"

DEFAULT_TOKEN_FILE = SCRIPT_DIR / "Tokens.txt"
DEFAULT_QUERY_FILE = CONFIG_DIR / "language_queries.json"
DEFAULT_REVIEW_SCHEMA_FILE = CONFIG_DIR / "review_schema.json"
DEFAULT_LIMITS_FILE = CONFIG_DIR / "collection_limits.json"
DEFAULT_TOKEN_ENV_VAR = "GITHUB_PAT_TOKEN"
DEFAULT_INSECURE_SSL_ENV_VAR = "PORTA_BENCH_INSECURE_SSL"

SUBTYPE_LANGUAGE_DEFAULTS = {
    "py2_py3": {
        "source_language": "python",
        "target_language": "python",
        "source_version": "2",
        "target_version": "3",
    },
    "cpp_python": {
        "source_language": "c++",
        "target_language": "python",
        "source_version": "",
        "target_version": "",
    },
    "java_python": {
        "source_language": "java",
        "target_language": "python",
        "source_version": "",
        "target_version": "",
    },
    "python_cpp": {
        "source_language": "python",
        "target_language": "c++",
        "source_version": "",
        "target_version": "",
    },
    "python_java": {
        "source_language": "python",
        "target_language": "java",
        "source_version": "",
        "target_version": "",
    },
}

REVIEW_FIELDS = [
    "instance_id",
    "scenario",
    "subtype",
    "subtype_zh",
    "migration_type",
    "migration_type_zh",
    "implementation_scope",
    "implementation_scope_zh",
    "logic_equivalence_scope",
    "logic_equivalence_scope_zh",
    "repo_full_name",
    "repo_created_at",
    "repo_stars",
    "pr_number",
    "pr_url",
    "pr_created_at",
    "title",
    "body_summary",
    "changed_file_summary",
    "has_tests_before",
    "has_tests_before_zh",
    "adds_new_tests",
    "adds_new_tests_zh",
    "auto_signals",
    "auto_signals_zh",
    "manual_label",
    "manual_label_zh",
    "source_language",
    "source_language_zh",
    "target_language",
    "target_language_zh",
    "source_version",
    "target_version",
    "migration_pattern",
    "test_framework",
    "test_framework_zh",
    "build_system",
    "build_system_zh",
    "reproducible",
    "reproducible_zh",
    "issue_rewrite_ready",
    "issue_rewrite_ready_zh",
    "leakage_risk",
    "leakage_risk_zh",
    "exclude_reason",
    "exclude_reason_zh",
    "reviewer",
    "cross_check_status",
    "cross_check_status_zh",
    "notes",
]

REVIEW_HEADER_ZH = {
    "instance_id": "样本ID",
    "scenario": "场景",
    "subtype": "子类型(英文)",
    "subtype_zh": "子类型(中文)",
    "migration_type": "迁移类型(英文)",
    "migration_type_zh": "迁移类型(中文)",
    "implementation_scope": "实现范围(英文)",
    "implementation_scope_zh": "实现范围(中文)",
    "logic_equivalence_scope": "逻辑对应范围(英文)",
    "logic_equivalence_scope_zh": "逻辑对应范围(中文)",
    "repo_full_name": "仓库名",
    "repo_created_at": "仓库创建时间",
    "repo_stars": "仓库Stars",
    "pr_number": "PR编号",
    "pr_url": "PR链接",
    "pr_created_at": "PR创建时间",
    "title": "标题",
    "body_summary": "PR摘要",
    "changed_file_summary": "修改文件摘要",
    "has_tests_before": "原始是否有测试(英文)",
    "has_tests_before_zh": "原始是否有测试(中文)",
    "adds_new_tests": "是否新增测试(英文)",
    "adds_new_tests_zh": "是否新增测试(中文)",
    "auto_signals": "自动信号(英文)",
    "auto_signals_zh": "自动信号(中文)",
    "manual_label": "人工标签(英文)",
    "manual_label_zh": "人工标签(中文提示)",
    "source_language": "源语言(英文)",
    "source_language_zh": "源语言(中文)",
    "target_language": "目标语言(英文)",
    "target_language_zh": "目标语言(中文)",
    "source_version": "源版本",
    "target_version": "目标版本",
    "migration_pattern": "迁移模式说明",
    "test_framework": "测试框架(英文)",
    "test_framework_zh": "测试框架(中文)",
    "build_system": "构建系统(英文)",
    "build_system_zh": "构建系统(中文)",
    "reproducible": "可复现性(英文)",
    "reproducible_zh": "可复现性(中文)",
    "issue_rewrite_ready": "是否适合改写Issue(英文)",
    "issue_rewrite_ready_zh": "是否适合改写Issue(中文)",
    "leakage_risk": "泄漏风险(英文)",
    "leakage_risk_zh": "泄漏风险(中文)",
    "exclude_reason": "排除原因(英文)",
    "exclude_reason_zh": "排除原因(中文)",
    "reviewer": "标注人",
    "cross_check_status": "交叉复核状态(英文)",
    "cross_check_status_zh": "交叉复核状态(中文)",
    "notes": "备注",
}

SUBTYPE_ZH = {
    "py2_py3": "Python 2 到 Python 3",
    "cpp_python": "C/C++ 到 Python",
    "java_python": "Java 到 Python",
    "python_cpp": "Python 到 C/C++",
    "python_java": "Python 到 Java",
}

VALUE_TRANSLATIONS = {
    "migration_type": {
        "version_migration": "同语言版本迁移",
        "cross_language_migration": "跨语言迁移",
    },
    "implementation_scope": {
        "partial_feature_migration": "部分功能迁移",
        "full_repo_translation": "整仓翻译/整体迁移",
        "not_applicable": "不适用",
    },
    "logic_equivalence_scope": {
        "same_logic_translation": "同一逻辑翻译",
        "partial_logic_replacement": "部分逻辑替换",
        "unclear_logic_mapping": "逻辑映射不清",
    },
    "bool": {
        "true": "是",
        "false": "否",
    },
    "manual_label": {
        "positive": "正例",
        "negative": "负例",
        "uncertain": "不确定",
    },
    "language": {
        "python": "Python",
        "c++": "C/C++",
        "java": "Java",
    },
    "test_framework": {
        "pytest": "pytest",
        "unittest": "unittest",
        "nose": "nose",
        "doctest": "doctest",
        "test_files_detected": "检测到测试文件",
    },
    "build_system": {
        "cmake": "CMake",
        "setuptools": "setuptools",
        "poetry": "Poetry",
        "tox": "tox",
        "make": "Make",
        "meson": "Meson",
    },
    "reproducible": {
        "yes": "可复现",
        "no": "不可复现",
        "unknown": "待确认",
    },
    "issue_rewrite_ready": {
        "yes": "可直接改写 issue",
        "needs_check": "还需检查",
        "no": "暂不适合",
    },
    "leakage_risk": {
        "low": "低",
        "medium": "中",
        "high": "高",
    },
    "cross_check_status": {
        "pending": "待复核",
        "checked": "已复核",
        "disagreed": "有分歧",
    },
    "auto_signal": {
        "keyword_match": "命中关键词",
        "strict_migration_signal": "强迁移信号",
        "adds_new_tests": "新增测试",
        "touches_tests": "修改测试",
        "touches_code": "修改代码",
        "bot_generated": "疑似机器人生成",
        "py3_only_support_noise": "仅 Python 3.x 支持噪声",
        "binding_wrapper_noise": "绑定/包装层噪声",
        "python_cpp_wrapper_noise": "Python-C++ 包装层噪声",
        "java_python_bridge_noise": "Java-Python 桥接噪声",
        "python_java_bridge_noise": "Python-Java 桥接噪声",
        "cross_language_mapping_visible": "可见跨语言对应关系",
    },
    "exclude_reason": {
        "too_many_commits": "提交数过多",
        "doc_only": "仅文档改动",
        "ci_only": "仅 CI 改动",
        "dependency_only": "仅依赖变更",
        "no_code_changes": "没有核心代码改动",
        "bot_generated_pr": "机器人生成 PR",
        "py3_only_support_noise": "仅 Python 3.x 支持噪声",
        "binding_wrapper_noise": "绑定/包装层噪声",
        "python_cpp_wrapper_noise": "Python-C++ 包装层噪声",
        "java_python_bridge_noise": "Java-Python 桥接噪声",
        "python_java_bridge_noise": "Python-Java 桥接噪声",
    },
}

DOC_FILE_RE = re.compile(r"(^|/)(docs?|docsrc|documentation)(/|$)|\.(md|rst|txt|adoc)$", re.IGNORECASE)
CI_FILE_RE = re.compile(r"(^|/)\.github/workflows/|(^|/)\.circleci/|(^|/)ci/|(^|/)azure-pipelines", re.IGNORECASE)
DEPENDENCY_FILE_RE = re.compile(
    r"(^|/)(requirements[^/]*|constraints[^/]*|Pipfile(\.lock)?|poetry\.lock|pyproject\.toml|setup\.(py|cfg)|environment\.ya?ml|tox\.ini|package-lock\.json|conda\.ya?ml)$",
    re.IGNORECASE,
)
TEST_PATH_RE = re.compile(
    r"(^|/)(tests?|testdata|testing)(/|$)|(^|/)(test_[^/]+\.py|[^/]+_test\.py)$|pytest|unittest",
    re.IGNORECASE,
)
POSITIVE_SIGNAL_RE = {
    "py2_py3": re.compile(
        r"python\s*2|python\s*3|py2|py3|2to3|futurize|six|iteritems|xrange|unicode|bytes|compatib",
        re.IGNORECASE,
    ),
    "cpp_python": re.compile(
        r"rewrite in python|port to python|convert to python|c\+\+\s+to\s+python|from c\+\+\s+to\s+python|replace c\+\+\s+with python",
        re.IGNORECASE,
    ),
    "java_python": re.compile(
        r"rewrite in python|port to python|convert to python|java\s+to\s+python|from\s+java\s+to\s+python|replace\s+java\s+with\s+python",
        re.IGNORECASE,
    ),
    "python_cpp": re.compile(
        r"rewrite in c\+\+|port to c\+\+|convert to c\+\+|python\s+to\s+c\+\+|from\s+python\s+to\s+c\+\+|replace\s+python\s+with\s+c\+\+|rewrite core in c\+\+",
        re.IGNORECASE,
    ),
    "python_java": re.compile(
        r"rewrite in java|port to java|convert to java|python\s+to\s+java|from\s+python\s+to\s+java|replace\s+python\s+with\s+java",
        re.IGNORECASE,
    ),
}
STRICT_SIGNAL_RE = {
    "py2_py3": re.compile(
        r"python\s*2.+python\s*3|drop\s+python\s*2|remove\s+python\s*2|port\s+to\s+python\s*3|migrate\s+to\s+python\s*3|2to3|futurize|xrange|iteritems|raw_input|basestring|__future__",
        re.IGNORECASE,
    ),
    "cpp_python": re.compile(
        r"rewrite\s+(?:.+\s+)?in\s+python|port\s+(?:.+\s+)?to\s+python|convert\s+(?:.+\s+)?to\s+python|reimplement\s+(?:.+\s+)?in\s+python|from\s+c\+\+\s+to\s+python|c\+\+\s+to\s+python|replace\s+c\+\+\s+with\s+python",
        re.IGNORECASE,
    ),
    "java_python": re.compile(
        r"rewrite\s+(?:.+\s+)?in\s+python|port\s+(?:.+\s+)?to\s+python|convert\s+(?:.+\s+)?to\s+python|reimplement\s+(?:.+\s+)?in\s+python|from\s+java\s+to\s+python|java\s+to\s+python|replace\s+java\s+with\s+python",
        re.IGNORECASE,
    ),
    "python_cpp": re.compile(
        r"rewrite\s+(?:.+\s+)?in\s+c\+\+|port\s+(?:.+\s+)?to\s+c\+\+|convert\s+(?:.+\s+)?to\s+c\+\+|reimplement\s+(?:.+\s+)?in\s+c\+\+|from\s+python\s+to\s+c\+\+|python\s+to\s+c\+\+|replace\s+python\s+with\s+c\+\+|move\s+hot\s+path\s+to\s+c\+\+|rewrite\s+core\s+in\s+c\+\+",
        re.IGNORECASE,
    ),
    "python_java": re.compile(
        r"rewrite\s+(?:.+\s+)?in\s+java|port\s+(?:.+\s+)?to\s+java|convert\s+(?:.+\s+)?to\s+java|reimplement\s+(?:.+\s+)?in\s+java|from\s+python\s+to\s+java|python\s+to\s+java|replace\s+python\s+with\s+java",
        re.IGNORECASE,
    ),
}
PY3_ONLY_SUPPORT_RE = re.compile(
    r"(support|add|enable|declare|compatibility|compatible).{0,30}python\s*3\.(1[0-9]|[4-9])|python\s*3\.(1[0-9]|[4-9]).{0,30}(support|compatibility|compatible)",
    re.IGNORECASE,
)
BOT_PR_RE = re.compile(r"dependabot|renovate|mend|generated by railway|\[bot\]", re.IGNORECASE)
CPP_WRAPPER_NOISE_RE = re.compile(r"pybind|pybind11|binding|bindings|wrapper|wrapping|swig|ctypes|cffi|ffi|cython", re.IGNORECASE)
PYTHON_FILE_RE = re.compile(r"\.py(i)?$", re.IGNORECASE)
CPP_FILE_RE = re.compile(r"\.(c|cc|cpp|cxx|h|hh|hpp|hxx)$", re.IGNORECASE)
JAVA_FILE_RE = re.compile(r"\.java$", re.IGNORECASE)
JAVA_PYTHON_BRIDGE_NOISE_RE = re.compile(r"jni|jython|py4j|jpype|gateway|bridge|sdk|client", re.IGNORECASE)
BUILD_HINTS = {
    "cmake": re.compile(r"(^|/)(cmakelists\.txt|cmake/)", re.IGNORECASE),
    "setuptools": re.compile(r"(^|/)(setup\.py|setup\.cfg)$", re.IGNORECASE),
    "poetry": re.compile(r"(^|/)poetry\.lock$|(^|/)pyproject\.toml$", re.IGNORECASE),
    "tox": re.compile(r"(^|/)tox\.ini$", re.IGNORECASE),
    "make": re.compile(r"(^|/)makefile$", re.IGNORECASE),
    "meson": re.compile(r"(^|/)(meson\.build|meson_options\.txt)$", re.IGNORECASE),
}
TEST_FRAMEWORK_HINTS = {
    "pytest": re.compile(r"pytest|(^|/)(conftest\.py|pytest\.ini)$", re.IGNORECASE),
    "unittest": re.compile(r"unittest", re.IGNORECASE),
    "nose": re.compile(r"\bnose\b", re.IGNORECASE),
    "doctest": re.compile(r"doctest", re.IGNORECASE),
}


def ensure_layout() -> None:
    for path in (
        CONFIG_DIR,
        DATA_DIR,
        RAW_DIR,
        SEARCH_RESULTS_DIR,
        PR_METADATA_DIR,
        SNAPSHOT_DIR,
        REVIEW_DIR,
        PROCESSED_DIR,
        STATS_DIR,
        LOG_DIR,
        TMP_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    items: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                items.append(json.loads(line))
    return items


def dump_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def truncate_text(text: str, width: int = 400) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return textwrap.shorten(text, width=width, placeholder="...") if text else ""


def repo_slug(full_name: str) -> str:
    return full_name.replace("/", "__")


def build_instance_id(subtype: str, repo_full_name: str, pr_number: int) -> str:
    return f"{subtype}__{repo_slug(repo_full_name)}__pr{pr_number}"


def infer_migration_type(subtype: str) -> str:
    if subtype == "py2_py3":
        return "version_migration"
    return "cross_language_migration"


def default_implementation_scope(subtype: str) -> str:
    if subtype == "py2_py3":
        return "not_applicable"
    return ""


def default_logic_equivalence_scope(subtype: str) -> str:
    if subtype == "py2_py3":
        return "same_logic_translation"
    return ""


def translate_single(category: str, value: str) -> str:
    normalized = (value or "").strip().lower()
    if not normalized:
        return ""
    return VALUE_TRANSLATIONS.get(category, {}).get(normalized, value)


def translate_multi(category: str, value: str) -> str:
    parts = [part.strip() for part in (value or "").split(";") if part.strip()]
    if not parts:
        return ""
    translated = [translate_single(category, part) for part in parts]
    return "；".join(translated)


def translate_bool(value: Any) -> str:
    if isinstance(value, bool):
        return "是" if value else "否"
    normalized = str(value).strip().lower()
    return VALUE_TRANSLATIONS["bool"].get(normalized, "")


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Review file not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def write_csv_rows(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_review_display_csv(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    zh_fieldnames = [REVIEW_HEADER_ZH.get(name, name) for name in fieldnames]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=zh_fieldnames)
        writer.writeheader()
        for row in rows:
            zh_row = {REVIEW_HEADER_ZH.get(name, name): row.get(name, "") for name in fieldnames}
            writer.writerow(zh_row)


def normalize_path(value: str) -> str:
    return value.replace("\\", "/").strip()


def is_doc_path(path: str) -> bool:
    return bool(DOC_FILE_RE.search(normalize_path(path)))


def is_ci_path(path: str) -> bool:
    return bool(CI_FILE_RE.search(normalize_path(path)))


def is_dependency_path(path: str) -> bool:
    return bool(DEPENDENCY_FILE_RE.search(normalize_path(path)))


def is_test_path(path: str) -> bool:
    return bool(TEST_PATH_RE.search(normalize_path(path)))


def infer_test_framework(changed_paths: Sequence[str], combined_text: str) -> str:
    for framework, pattern in TEST_FRAMEWORK_HINTS.items():
        if any(pattern.search(path) for path in changed_paths) or pattern.search(combined_text):
            return framework
    if any(is_test_path(path) for path in changed_paths):
        return "test_files_detected"
    return ""


def infer_build_system(changed_paths: Sequence[str], combined_text: str) -> str:
    for build_system, pattern in BUILD_HINTS.items():
        if any(pattern.search(path) for path in changed_paths):
            return build_system
    lowered = combined_text.lower()
    if "cmake" in lowered:
        return "cmake"
    if "poetry" in lowered:
        return "poetry"
    if "setuptools" in lowered:
        return "setuptools"
    return ""


def infer_leakage_risk(title: str, body: str) -> str:
    combined = f"{title}\n{body}".lower()
    if any(keyword in combined for keyword in ("replace ", "convert ", "use six", "2to3", "xrange", "iteritems", "print(")):
        return "high"
    if any(keyword in combined for keyword in ("compatibility", "python 3", "port to python", "rewrite in python")):
        return "medium"
    return "low"


def progress_bar(done: int, total: int, width: int = 20) -> str:
    if total <= 0:
        return "[....................] 0%"
    ratio = max(0.0, min(1.0, done / total))
    filled = round(width * ratio)
    return f"[{'#' * filled}{'.' * (width - filled)}] {ratio * 100:.0f}%"


def default_languages(subtype: str) -> Dict[str, str]:
    return dict(SUBTYPE_LANGUAGE_DEFAULTS[subtype])


def touched_language_files(paths: Sequence[str], language: str) -> bool:
    normalized = [normalize_path(path) for path in paths]
    if language == "python":
        return any(PYTHON_FILE_RE.search(path) for path in normalized)
    if language == "c++":
        return any(CPP_FILE_RE.search(path) for path in normalized)
    if language == "java":
        return any(JAVA_FILE_RE.search(path) for path in normalized)
    return False


class GitHubClient:
    def __init__(self, token_file: Path, sleep_sec: float, max_retries: int, logger: logging.Logger):
        self.tokens = self._load_tokens(token_file)
        self.token_index = 0
        self.sleep_sec = sleep_sec
        self.max_retries = max_retries
        self.logger = logger
        self.session = self._create_session()

    def _load_tokens(self, token_file: Path) -> List[str]:
        tokens = []
        if token_file.exists():
            with token_file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        tokens.append(line)
        if not tokens:
            env_token = os.environ.get(DEFAULT_TOKEN_ENV_VAR) or os.environ.get("GITHUB_PAT")
            if env_token:
                tokens.append(env_token.strip())
        return tokens

    def _current_token(self) -> Optional[str]:
        if not self.tokens:
            return None
        return self.tokens[self.token_index]

    def _switch_token(self) -> bool:
        if len(self.tokens) <= 1:
            return False
        self.token_index = (self.token_index + 1) % len(self.tokens)
        token = self._current_token()
        if token:
            self.session.headers["Authorization"] = f"token {token}"
        return True

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "PortaBench-Language-Migration-Collector",
                "Accept": "application/vnd.github.v3+json",
            }
        )
        token = self._current_token()
        if token:
            session.headers["Authorization"] = f"token {token}"
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        if os.environ.get(DEFAULT_INSECURE_SSL_ENV_VAR, "").strip().lower() in {"1", "true", "yes", "on"}:
            session.verify = False
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.logger.warning(
                "%s enabled: HTTPS certificate verification is temporarily disabled for this session.",
                DEFAULT_INSECURE_SSL_ENV_VAR,
            )
        return session

    def _handle_rate_limit(self, response: requests.Response) -> bool:
        if response.status_code != 403:
            return False
        if response.headers.get("X-RateLimit-Remaining") != "0":
            return False
        if self._switch_token():
            self.logger.warning("GitHub API rate limit reached, switched token.")
            return True
        reset_ts = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait_sec = max(5, min(120, reset_ts - int(time.time()) + 5))
        self.logger.warning("GitHub API rate limit reached, sleeping %ss.", wait_sec)
        time.sleep(wait_sec)
        return True

    def _request(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=30)
                if response.status_code == 403 and self._handle_rate_limit(response):
                    continue
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                if self.sleep_sec:
                    time.sleep(self.sleep_sec)
                return response.json()
            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    raise RuntimeError(f"GitHub API request failed: {url}") from exc
                self.logger.warning("GitHub API request failed (%s/%s): %s", attempt, self.max_retries, exc)
                time.sleep(min(2 ** attempt, 30))
        raise RuntimeError(f"GitHub API request failed after retries: {url}")

    def search_prs(self, query: str, page: int, per_page: int) -> Dict[str, Any]:
        return self._request(
            "https://api.github.com/search/issues",
            params={"q": query, "per_page": per_page, "page": page},
        )

    def fetch_pr_basic(self, repo_api_url: str, pr_number: int) -> Optional[Dict[str, Any]]:
        return self._request(f"{repo_api_url}/pulls/{pr_number}")

    def fetch_pr_files(self, owner: str, repo: str, pr_number: int) -> List[Dict[str, Any]]:
        files: List[Dict[str, Any]] = []
        page = 1
        while True:
            batch = self._request(
                f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files",
                params={"per_page": 100, "page": page},
            )
            if not batch:
                break
            files.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return files

    def fetch_pr_commits(self, owner: str, repo: str, pr_number: int) -> List[Dict[str, Any]]:
        commits: List[Dict[str, Any]] = []
        page = 1
        while True:
            batch = self._request(
                f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/commits",
                params={"per_page": 100, "page": page},
            )
            if not batch:
                break
            commits.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return commits


class PortaBenchCollector:
    def __init__(self, args: argparse.Namespace):
        ensure_layout()
        self.args = args
        if isinstance(self.args.query, list):
            self.args.query = " ".join(part for part in self.args.query if part).strip()
        self.subtype = args.subtype
        self.stage = args.stage
        self.queries_config = load_json(Path(args.query_file))
        self.review_schema = load_json(Path(args.review_schema_file))
        self.limits = load_json(Path(args.limits_file))
        self.logger = self._configure_logger()
        self.github = GitHubClient(
            token_file=Path(args.token_file),
            sleep_sec=args.sleep_sec if args.sleep_sec is not None else float(self.limits.get("sleep_sec", 1.0)),
            max_retries=int(self.limits.get("max_retries", 3)),
            logger=self.logger,
        )
        self.search_per_page = int(self.limits.get("search_per_page", 100))
        self.max_search_pages_per_query = int(
            args.max_search_pages if args.max_search_pages is not None else self.limits.get("max_search_pages_per_query", 10)
        )
        self.max_size_kb = int(self.limits.get("max_size_mb", 310)) * 1024
        self.max_commit_count = int(self.limits.get("max_commit_count", 20))
        self.min_stars = int(self.limits.get("min_stars", 5))
        self.max_collect_candidates = int(
            args.max_prs if args.max_prs is not None else self.limits.get("max_prs_per_subtype", 1000)
        )

    def _configure_logger(self) -> logging.Logger:
        logger_name = f"porta_bench_{self.stage}_{self.subtype}"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.handlers.clear()

        if self.stage == "package":
            log_file = LOG_DIR / "packaging.log"
        else:
            log_file = LOG_DIR / f"collect_{self.subtype}.log"

        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        return logger

    def subtype_search_dir(self) -> Path:
        return SEARCH_RESULTS_DIR / self.subtype

    def subtype_metadata_dir(self) -> Path:
        return PR_METADATA_DIR / self.subtype

    def subtype_snapshot_dir(self) -> Path:
        return SNAPSHOT_DIR / self.subtype

    def collect_manifest_path(self) -> Path:
        return self.subtype_metadata_dir() / "collect_index.jsonl"

    def enrich_manifest_path(self) -> Path:
        return self.subtype_metadata_dir() / "enriched_index.jsonl"

    def collect_checkpoint_path(self) -> Path:
        return self.subtype_search_dir() / "collect_checkpoint.json"

    def enrich_checkpoint_path(self) -> Path:
        return self.subtype_metadata_dir() / "enrich_checkpoint.json"

    def review_csv_path(self) -> Path:
        return REVIEW_DIR / f"{self.subtype}_manual_review.csv"

    def review_display_csv_path(self) -> Path:
        return REVIEW_DIR / f"{self.subtype}_manual_review_zh.csv"

    def processed_path(self) -> Path:
        return PROCESSED_DIR / f"{self.subtype}_candidates.jsonl"

    def stats_path(self) -> Path:
        return STATS_DIR / f"{self.subtype}_stats.json"

    def global_dataset_path(self) -> Path:
        return PROCESSED_DIR / "porta_lang_migration_v1.jsonl"

    def subtype_report_path(self) -> Path:
        return STATS_DIR / f"{self.subtype}_progress.md"

    def dashboard_path(self) -> Path:
        return PROJECT_ROOT / "项目进度看板.md"

    def query_specs(self) -> List[Dict[str, str]]:
        if self.args.query:
            query_hash = hashlib.md5(self.args.query.encode("utf-8")).hexdigest()[:10]
            return [{"name": f"adhoc_{query_hash}", "query": self.args.query}]
        config = self.queries_config[self.subtype]
        return list(config["queries"])

    def run(self) -> None:
        self.subtype_search_dir().mkdir(parents=True, exist_ok=True)
        self.subtype_metadata_dir().mkdir(parents=True, exist_ok=True)
        self.subtype_snapshot_dir().mkdir(parents=True, exist_ok=True)
        if self.stage in {"collect", "enrich"}:
            self.preflight_network_check()

        stage_map = {
            "collect": self.collect_stage,
            "enrich": self.enrich_stage,
            "export-review": self.export_review_stage,
            "apply-review": self.apply_review_stage,
            "package": self.package_stage,
        }
        try:
            stage_map[self.stage]()
        finally:
            self.write_visual_progress_report(self.stage)

    def load_collect_index(self) -> Dict[str, Dict[str, Any]]:
        items = load_jsonl(self.collect_manifest_path())
        return {item["instance_id"]: item for item in items}

    def save_collect_index(self, items: Dict[str, Dict[str, Any]]) -> None:
        ordered = sorted(items.values(), key=lambda row: (row["repo_full_name"], row["pr_number"]))
        dump_jsonl(self.collect_manifest_path(), ordered)

    def load_enrich_index(self) -> Dict[str, Dict[str, Any]]:
        items = load_jsonl(self.enrich_manifest_path())
        return {item["instance_id"]: item for item in items}

    def save_enrich_index(self, items: Dict[str, Dict[str, Any]]) -> None:
        ordered = sorted(items.values(), key=lambda row: (row["repo_full_name"], row["pr_number"]))
        dump_jsonl(self.enrich_manifest_path(), ordered)

    def load_checkpoint(self, path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
        if not path.exists():
            return default
        return load_json(path)

    def save_checkpoint(self, path: Path, payload: Dict[str, Any]) -> None:
        dump_json(path, payload)

    def should_keep_collect_candidate(self, pr_basic: Dict[str, Any]) -> Dict[str, Any]:
        repo = pr_basic["base"]["repo"]
        checks = {
            "merged": bool(pr_basic.get("merged_at")),
            "min_stars": int(repo.get("stargazers_count", 0)) >= self.min_stars,
            "public_license": bool(repo.get("license")),
            "size_ok": int(repo.get("size", 0)) <= self.max_size_kb,
        }
        keep = all(checks.values())
        exclude_reasons = [key for key, passed in checks.items() if not passed]
        return {"keep": keep, "checks": checks, "exclude_reasons": exclude_reasons}

    def collect_stage(self) -> None:
        self.logger.info("Starting collect stage for %s", self.subtype)
        checkpoint = self.load_checkpoint(self.collect_checkpoint_path(), {"pages": {}, "finished_queries": []})
        collect_index = self.load_collect_index()
        queries = self.query_specs()
        unique_seen = len(collect_index)
        candidate_count = sum(1 for item in collect_index.values() if item.get("collect_status") == "candidate")

        for query_spec in queries:
            query_name = query_spec["name"]
            query_string = query_spec["query"]
            if query_name in checkpoint["finished_queries"]:
                self.logger.info("Skipping completed query %s", query_name)
                continue

            query_dir = self.subtype_search_dir() / query_name
            query_dir.mkdir(parents=True, exist_ok=True)
            start_page = int(checkpoint["pages"].get(query_name, 0)) + 1

            for page in range(start_page, self.max_search_pages_per_query + 1):
                response = self.github.search_prs(query=query_string, page=page, per_page=self.search_per_page)
                items = response.get("items", [])
                dump_json(query_dir / f"page_{page:04d}.json", response)
                checkpoint["pages"][query_name] = page
                self.save_checkpoint(self.collect_checkpoint_path(), checkpoint)

                if not items:
                    self.logger.info("Query %s exhausted at page %s", query_name, page)
                    break

                for pr_stub in items:
                    pr_number = pr_stub["number"]
                    try:
                        basic = self.github.fetch_pr_basic(pr_stub["repository_url"], pr_number)
                    except Exception as exc:
                        self.logger.warning(
                            "Skipping PR after repeated detail fetch failure: %s#%s (%s)",
                            pr_stub.get("repository_url", "unknown_repo"),
                            pr_number,
                            exc,
                        )
                        continue
                    if not basic:
                        continue

                    repo = basic["base"]["repo"]
                    instance_id = build_instance_id(self.subtype, repo["full_name"], pr_number)
                    query_status = self.should_keep_collect_candidate(basic)
                    matched_queries = set(collect_index.get(instance_id, {}).get("matched_queries", []))
                    matched_queries.add(query_name)

                    current = collect_index.get(instance_id, {})
                    record = {
                        "instance_id": instance_id,
                        "scenario": SCENARIO,
                        "subtype": self.subtype,
                        "pr_id": basic["id"],
                        "repo_full_name": repo["full_name"],
                        "repo_api_url": pr_stub["repository_url"],
                        "pr_number": pr_number,
                        "pr_url": basic.get("html_url", pr_stub.get("html_url")),
                        "title": basic.get("title", ""),
                        "body": basic.get("body") or "",
                        "matched_queries": sorted(matched_queries),
                        "collect_status": "candidate" if query_status["keep"] else "excluded",
                        "collect_exclude_reasons": query_status["exclude_reasons"],
                        "collect_checks": query_status["checks"],
                    "repo_summary": {
                        "stars": repo.get("stargazers_count", 0),
                        "created_at": repo.get("created_at"),
                        "license": repo.get("license", {}).get("spdx_id") if repo.get("license") else None,
                        "size_kb": repo.get("size", 0),
                        "default_branch": repo.get("default_branch"),
                        "language": repo.get("language"),
                    },
                    "search_stub": {
                        "score": pr_stub.get("score"),
                        "state": pr_stub.get("state"),
                        "pr_created_at": basic.get("created_at"),
                        "created_at": pr_stub.get("created_at"),
                        "updated_at": pr_stub.get("updated_at"),
                    },
                }

                    if current and current.get("collect_status") == "candidate":
                        record["collect_status"] = "candidate"
                        record["collect_exclude_reasons"] = current.get("collect_exclude_reasons", [])

                    collect_index[instance_id] = record
                    unique_seen = len(collect_index)
                    candidate_count = sum(1 for item in collect_index.values() if item.get("collect_status") == "candidate")
                    if candidate_count >= self.max_collect_candidates:
                        self.logger.info("Reached max candidate limit: %s", self.max_collect_candidates)
                        break

                self.save_collect_index(collect_index)
                self.logger.info(
                    "Query %s page %s collected. Current unique PRs: %s, candidate PRs: %s",
                    query_name,
                    page,
                    unique_seen,
                    candidate_count,
                )
                if candidate_count >= self.max_collect_candidates:
                    break

            checkpoint["finished_queries"].append(query_name)
            self.save_checkpoint(self.collect_checkpoint_path(), checkpoint)
            if candidate_count >= self.max_collect_candidates:
                break

        self.logger.info("Collect stage finished. Unique PRs: %s", len(collect_index))

    def auto_filter_summary(self, subtype: str, pr_basic: Dict[str, Any], files: List[Dict[str, Any]], commits: List[Dict[str, Any]]) -> Dict[str, Any]:
        changed_paths = [item["filename"] for item in files]
        title = pr_basic.get("title", "")
        body = pr_basic.get("body") or ""
        pr_author = (pr_basic.get("user") or {}).get("login", "")
        patches = "\n".join((item.get("patch") or "")[:1500] for item in files[:20])
        combined_text = f"{title}\n{body}\n{patches}"

        doc_only = bool(changed_paths) and all(is_doc_path(path) for path in changed_paths)
        ci_only = bool(changed_paths) and all(is_ci_path(path) or is_doc_path(path) for path in changed_paths)
        dependency_only = bool(changed_paths) and all(is_dependency_path(path) or is_doc_path(path) for path in changed_paths)
        commit_count_ok = len(commits) <= self.max_commit_count
        positive_signal = bool(POSITIVE_SIGNAL_RE[subtype].search(combined_text))
        strict_signal = bool(STRICT_SIGNAL_RE[subtype].search(combined_text))
        adds_new_tests = any(item.get("status") == "added" and is_test_path(item["filename"]) for item in files)
        touches_tests = any(is_test_path(path) for path in changed_paths)
        touches_code = any(PYTHON_FILE_RE.search(path) or CPP_FILE_RE.search(path) or JAVA_FILE_RE.search(path) for path in changed_paths)
        bot_generated = bool(BOT_PR_RE.search(f"{title}\n{body}\n{pr_author}"))
        py3_only_support_noise = subtype == "py2_py3" and bool(PY3_ONLY_SUPPORT_RE.search(combined_text)) and not strict_signal
        binding_wrapper_noise = subtype == "cpp_python" and bool(CPP_WRAPPER_NOISE_RE.search(combined_text)) and not strict_signal
        python_cpp_wrapper_noise = subtype == "python_cpp" and bool(CPP_WRAPPER_NOISE_RE.search(combined_text)) and not strict_signal
        java_python_bridge_noise = subtype == "java_python" and bool(JAVA_PYTHON_BRIDGE_NOISE_RE.search(combined_text)) and not strict_signal
        python_java_bridge_noise = subtype == "python_java" and bool(JAVA_PYTHON_BRIDGE_NOISE_RE.search(combined_text)) and not strict_signal
        defaults = default_languages(subtype)
        touches_source_language = touched_language_files(changed_paths, defaults["source_language"])
        touches_target_language = touched_language_files(changed_paths, defaults["target_language"])
        cross_language_mapping_visible = (
            subtype != "py2_py3" and touches_source_language and touches_target_language
        )

        signals = []
        if positive_signal:
            signals.append("keyword_match")
        if strict_signal:
            signals.append("strict_migration_signal")
        if adds_new_tests:
            signals.append("adds_new_tests")
        if touches_tests:
            signals.append("touches_tests")
        if touches_code:
            signals.append("touches_code")
        if bot_generated:
            signals.append("bot_generated")
        if py3_only_support_noise:
            signals.append("py3_only_support_noise")
        if binding_wrapper_noise:
            signals.append("binding_wrapper_noise")
        if python_cpp_wrapper_noise:
            signals.append("python_cpp_wrapper_noise")
        if java_python_bridge_noise:
            signals.append("java_python_bridge_noise")
        if python_java_bridge_noise:
            signals.append("python_java_bridge_noise")
        if cross_language_mapping_visible:
            signals.append("cross_language_mapping_visible")

        exclude_reasons = []
        if not commit_count_ok:
            exclude_reasons.append("too_many_commits")
        if doc_only:
            exclude_reasons.append("doc_only")
        if ci_only:
            exclude_reasons.append("ci_only")
        if dependency_only and re.search(r"\b(bump|upgrade|pin|version)\b", combined_text, re.IGNORECASE):
            exclude_reasons.append("dependency_only")
        if not touches_code and not doc_only and not ci_only:
            exclude_reasons.append("no_code_changes")
        if bot_generated:
            exclude_reasons.append("bot_generated_pr")
        if py3_only_support_noise:
            exclude_reasons.append("py3_only_support_noise")
        if binding_wrapper_noise:
            exclude_reasons.append("binding_wrapper_noise")
        if python_cpp_wrapper_noise:
            exclude_reasons.append("python_cpp_wrapper_noise")
        if java_python_bridge_noise:
            exclude_reasons.append("java_python_bridge_noise")
        if python_java_bridge_noise:
            exclude_reasons.append("python_java_bridge_noise")

        return {
            "changed_paths": changed_paths,
            "commit_count_ok": commit_count_ok,
            "adds_new_tests": adds_new_tests,
            "touches_tests": touches_tests,
            "touches_code": touches_code,
            "touches_source_language": touches_source_language,
            "touches_target_language": touches_target_language,
            "cross_language_mapping_visible": cross_language_mapping_visible,
            "auto_signals": sorted(set(signals)),
            "exclude_reasons": exclude_reasons,
        }

    def infer_stage_status(self) -> Dict[str, Dict[str, str]]:
        collect_records = list(self.load_collect_index().values())
        enrich_records = list(self.load_enrich_index().values())
        review_exists = self.review_csv_path().exists()
        review_rows = read_csv_rows(self.review_csv_path()) if review_exists else []
        processed_records = load_jsonl(self.processed_path())
        stats_exists = self.stats_path().exists()
        collect_candidate_count = sum(1 for item in collect_records if item.get("collect_status") == "candidate")
        enrich_finished_count = sum(1 for item in enrich_records if item.get("auto_status") in {"candidate", "excluded", "error"})
        labeled_count = sum(1 for row in review_rows if (row.get("manual_label") or "").strip())

        def stage_info(done: bool, partial: bool, detail: str) -> Dict[str, str]:
            if done:
                return {"state": "done", "detail": detail}
            if partial:
                return {"state": "partial", "detail": detail}
            return {"state": "pending", "detail": detail}

        return {
            "collect": stage_info(bool(collect_records), False, f"unique={len(collect_records)}"),
            "enrich": stage_info(
                collect_candidate_count > 0 and enrich_finished_count >= collect_candidate_count,
                bool(enrich_records),
                f"records={enrich_finished_count}/{collect_candidate_count}",
            ),
            "export-review": stage_info(review_exists, review_exists, f"rows={len(review_rows)}"),
            "apply-review": stage_info(bool(processed_records), bool(labeled_count), f"processed={len(processed_records)}"),
            "package": stage_info(stats_exists, bool(processed_records) or review_exists, f"stats={'yes' if stats_exists else 'no'}"),
        }

    def write_visual_progress_report(self, trigger_stage: str) -> None:
        collect_records = list(self.load_collect_index().values())
        enrich_records = list(self.load_enrich_index().values())
        review_rows = read_csv_rows(self.review_csv_path()) if self.review_csv_path().exists() else []
        processed_records = load_jsonl(self.processed_path())
        stats = self.compute_stats(review_rows)
        stage_status = self.infer_stage_status()
        candidate_collect_count = sum(1 for item in collect_records if item.get("collect_status") == "candidate")
        enrich_candidate_count = sum(1 for item in enrich_records if item.get("auto_status") == "candidate")
        enrich_finished_count = sum(1 for item in enrich_records if item.get("auto_status") in {"candidate", "excluded", "error"})
        enrich_error_records = [item for item in enrich_records if item.get("auto_status") == "error"]
        manual_labeled_count = sum(1 for row in review_rows if (row.get("manual_label") or "").strip())
        total_review_rows = len(review_rows)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            f"# {self.subtype} 进度看板",
            "",
            f"- 更新时间: `{now}`",
            f"- 触发节点: `{trigger_stage}`",
            "",
            "## 阶段状态",
            "",
            "| 节点 | 状态 | 说明 |",
            "| --- | --- | --- |",
        ]
        for stage in SUPPORTED_STAGES:
            info = stage_status[stage]
            lines.append(f"| `{stage}` | `{info['state']}` | {info['detail']} |")

        lines.extend(
            [
                "",
                "## 数量概览",
                "",
                f"- Collect 候选: {candidate_collect_count}/{len(collect_records)} {progress_bar(candidate_collect_count, max(len(collect_records), 1))}",
                f"- Enrich 完成: {enrich_finished_count}/{candidate_collect_count} {progress_bar(enrich_finished_count, max(candidate_collect_count, 1))}",
                f"- Enrich 候选: {enrich_candidate_count}/{max(enrich_finished_count, 0)} {progress_bar(enrich_candidate_count, max(enrich_finished_count, 1))}",
                f"- 人工已标注: {manual_labeled_count}/{total_review_rows} {progress_bar(manual_labeled_count, max(total_review_rows, 1))}",
                f"- Processed 保留: {len(processed_records)}/{total_review_rows} {progress_bar(len(processed_records), max(total_review_rows, 1))}",
                "",
                "## 中间产物",
                "",
                f"- Collect 索引: `{self.collect_manifest_path()}`",
                f"- Enrich 索引: `{self.enrich_manifest_path()}`",
                f"- Review CSV: `{self.review_csv_path()}`",
                f"- Processed JSONL: `{self.processed_path()}`",
                f"- Stats JSON: `{self.stats_path()}`",
                "",
                "## 当前阻塞",
                "",
            ]
        )
        if enrich_error_records:
            for item in enrich_error_records[:5]:
                lines.append(f"- `{item['instance_id']}`: {item.get('enrich_error', 'unknown error')}")
        else:
            lines.append("- 暂无已记录的 enrich 错误。")

        lines.extend(
            [
                "",
                "## 统计快照",
                "",
                f"- raw_pr_count: `{stats['raw_pr_count']}`",
                f"- unique_pr_count: `{stats['unique_pr_count']}`",
                f"- collect_candidate_count: `{stats['collect_candidate_count']}`",
                f"- auto_filtered_candidate_count: `{stats['auto_filtered_candidate_count']}`",
                f"- auto_excluded_count: `{stats['auto_excluded_count']}`",
                f"- enrich_error_count: `{stats['enrich_error_count']}`",
                f"- manual_positive_count: `{stats['manual_positive_count']}`",
                f"- manual_negative_count: `{stats['manual_negative_count']}`",
                f"- manual_uncertain_count: `{stats['manual_uncertain_count']}`",
                f"- processed_candidate_count: `{stats['processed_candidate_count']}`",
            ]
        )
        self.subtype_report_path().write_text("\n".join(lines) + "\n", encoding="utf-8")

        board_lines = [
            "# 项目进度看板",
            "",
            f"- 更新时间: `{now}`",
            "",
            "| 子场景 | unique PR | collect 候选 | enrich 候选 | processed | 看板 |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for subtype in SUPPORTED_SUBTYPES:
            collect_items = load_jsonl(PR_METADATA_DIR / subtype / "collect_index.jsonl")
            enrich_items = load_jsonl(PR_METADATA_DIR / subtype / "enriched_index.jsonl")
            processed_items = load_jsonl(PROCESSED_DIR / f"{subtype}_candidates.jsonl")
            board_lines.append(
                "| `{subtype}` | `{unique}` | `{collect}` | `{enrich}` | `{processed}` | `{report}` |".format(
                    subtype=subtype,
                    unique=len(collect_items),
                    collect=sum(1 for item in collect_items if item.get("collect_status") == "candidate"),
                    enrich=sum(1 for item in enrich_items if item.get("auto_status") == "candidate"),
                    processed=len(processed_items),
                    report=(STATS_DIR / f"{subtype}_progress.md").as_posix(),
                )
            )
        self.dashboard_path().write_text("\n".join(board_lines) + "\n", encoding="utf-8")

    def preflight_network_check(self) -> None:
        diagnostics = self.collect_github_network_diagnostics()
        resolved_ips = diagnostics.get("github_com_resolved_ips", [])
        proxy_reachable = bool(diagnostics.get("reachable_proxy"))
        if any(ip.startswith("127.") for ip in resolved_ips) and not proxy_reachable:
            self.logger.error("Preflight network check failed: %s", json.dumps(diagnostics, ensure_ascii=False))
            raise RuntimeError(
                "GitHub connectivity preflight failed: github.com resolves to 127.x.x.x. "
                "Please remove GitHub-related hosts overrides before running collect/enrich."
            )
        if any(ip.startswith("127.") for ip in resolved_ips) and proxy_reachable:
            self.logger.warning(
                "GitHub resolves to loopback, but a reachable proxy is configured. Continuing with proxy: %s",
                json.dumps(diagnostics.get("reachable_proxy"), ensure_ascii=False),
            )

    def clone_repo(self, clone_url: str, workdir: Path) -> None:
        result = subprocess.run(
            ["git", "clone", "--no-checkout", "--filter=blob:none", clone_url, str(workdir)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"git clone failed: {result.stderr.strip()}")

    def git_config_value(self, key: str) -> str:
        result = subprocess.run(
            ["git", "config", "--global", "--get", key],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return ""
        return result.stdout.strip()

    def collect_github_network_diagnostics(self) -> Dict[str, Any]:
        try:
            resolved_ips = sorted(
                {
                    item[4][0]
                    for item in socket.getaddrinfo("github.com", 443, proto=socket.IPPROTO_TCP)
                    if item and item[4]
                }
            )
        except OSError as exc:
            resolved_ips = [f"dns_error:{exc}"]

        hosts_hits: List[str] = []
        hosts_path = Path(r"C:\Windows\System32\drivers\etc\hosts")
        if hosts_path.exists():
            try:
                for raw_line in hosts_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                    stripped = raw_line.strip()
                    if not stripped or stripped.startswith("#"):
                        continue
                    if any(domain in stripped for domain in ("github.com", "api.github.com", "raw.githubusercontent.com")):
                        hosts_hits.append(stripped)
            except OSError as exc:
                hosts_hits.append(f"hosts_read_error:{exc}")

        git_version = subprocess.run(
            ["git", "--version"],
            capture_output=True,
            text=True,
        )

        proxies = [
            os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY") or "",
            os.environ.get("http_proxy") or os.environ.get("HTTP_PROXY") or "",
            self.git_config_value("https.proxy"),
            self.git_config_value("http.proxy"),
        ]
        reachable_proxy: Dict[str, Any] = {}
        for proxy in proxies:
            if not proxy:
                continue
            parsed = urlparse(proxy)
            host = parsed.hostname
            port = parsed.port
            if not host or not port:
                continue
            try:
                with socket.create_connection((host, port), timeout=2):
                    reachable_proxy = {"proxy": proxy, "host": host, "port": port}
                    break
            except OSError:
                continue

        return {
            "github_com_resolved_ips": resolved_ips,
            "git_version": git_version.stdout.strip(),
            "git_http_proxy": self.git_config_value("http.proxy"),
            "git_https_proxy": self.git_config_value("https.proxy"),
            "env_http_proxy": os.environ.get("http_proxy") or os.environ.get("HTTP_PROXY") or "",
            "env_https_proxy": os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY") or "",
            "hosts_overrides": hosts_hits[:10],
            "reachable_proxy": reachable_proxy,
        }

    def ensure_commit_available(self, repo_dir: Path, sha: str) -> None:
        check = subprocess.run(
            ["git", "cat-file", "-e", sha],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        if check.returncode == 0:
            return
        fetch = subprocess.run(
            ["git", "fetch", "--depth", "1", "origin", sha],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        if fetch.returncode != 0:
            raise RuntimeError(f"git fetch {sha} failed: {fetch.stderr.strip()}")

    def checkout_and_copy_snapshot(self, repo_dir: Path, sha: str, destination: Path) -> None:
        if destination.exists():
            return
        self.ensure_commit_available(repo_dir, sha)
        checkout = subprocess.run(
            ["git", "checkout", "--force", sha],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        if checkout.returncode != 0:
            raise RuntimeError(f"git checkout {sha} failed: {checkout.stderr.strip()}")
        shutil.copytree(
            repo_dir,
            destination,
            ignore=shutil.ignore_patterns(".git", "__pycache__", ".pytest_cache", ".mypy_cache"),
        )

    def repo_has_tests(self, repo_root: Path) -> bool:
        if not repo_root.exists():
            return False
        for file_path in repo_root.rglob("*"):
            if file_path.is_file() and is_test_path(file_path.relative_to(repo_root).as_posix()):
                return True
        return False

    def metadata_file_path(self, instance_id: str) -> Path:
        return self.subtype_metadata_dir() / f"{instance_id}.json"

    def enrich_stage(self) -> None:
        self.logger.info("Starting enrich stage for %s", self.subtype)
        collect_index = self.load_collect_index()
        enrich_index = self.load_enrich_index()
        checkpoint = self.load_checkpoint(self.enrich_checkpoint_path(), {"processed_instance_ids": []})
        processed_ids = set(checkpoint["processed_instance_ids"])

        for instance_id, record in sorted(collect_index.items()):
            if record.get("collect_status") != "candidate":
                continue
            if instance_id in processed_ids and instance_id in enrich_index:
                continue

            try:
                pr_basic = self.github.fetch_pr_basic(record["repo_api_url"], record["pr_number"])
                if not pr_basic:
                    raise RuntimeError("PR detail not found")
                repo = pr_basic["base"]["repo"]
                owner = repo["owner"]["login"]
                repo_name = repo["name"]
                files = self.github.fetch_pr_files(owner, repo_name, record["pr_number"])
                commits = self.github.fetch_pr_commits(owner, repo_name, record["pr_number"])

                filter_result = self.auto_filter_summary(self.subtype, pr_basic, files, commits)
                base_sha = (
                    commits[0]["parents"][0]["sha"]
                    if commits and commits[0].get("parents")
                    else pr_basic["base"]["sha"]
                )
                final_sha = commits[-1]["sha"] if commits else (pr_basic.get("merge_commit_sha") or pr_basic["head"]["sha"])

                snapshot_root = self.subtype_snapshot_dir() / f"{repo_slug(repo['full_name'])}__pr{record['pr_number']}__{instance_id}"
                r0_path = snapshot_root / "r0"
                rn_path = snapshot_root / "rn"

                has_tests_before = False
                snapshot_error = ""
                if not filter_result["exclude_reasons"]:
                    with tempfile.TemporaryDirectory(prefix="porta_bench_", dir=str(TMP_DIR)) as temp_dir:
                        repo_dir = Path(temp_dir) / "repo"
                        self.clone_repo(repo["clone_url"], repo_dir)
                        self.checkout_and_copy_snapshot(repo_dir, base_sha, r0_path)
                        self.checkout_and_copy_snapshot(repo_dir, final_sha, rn_path)
                    has_tests_before = self.repo_has_tests(r0_path)
                    if not has_tests_before:
                        filter_result["exclude_reasons"].append("no_tests_detected")
                else:
                    snapshot_error = "skipped_snapshot_due_to_auto_filter"

                status = "candidate" if not filter_result["exclude_reasons"] else "excluded"
                metadata = {
                    "instance_id": instance_id,
                    "scenario": SCENARIO,
                    "subtype": self.subtype,
                    "repo_full_name": repo["full_name"],
                    "pr_number": record["pr_number"],
                    "pr_url": pr_basic.get("html_url"),
                    "repo": {
                        "full_name": repo["full_name"],
                        "clone_url": repo["clone_url"],
                        "default_branch": repo.get("default_branch"),
                        "created_at": repo.get("created_at"),
                        "stars": repo.get("stargazers_count"),
                        "license": repo.get("license", {}).get("spdx_id") if repo.get("license") else None,
                        "size_kb": repo.get("size"),
                    },
                    "pull_request": {
                        "id": pr_basic.get("id"),
                        "title": pr_basic.get("title", ""),
                        "body": pr_basic.get("body") or "",
                        "created_at": pr_basic.get("created_at"),
                        "merged_at": pr_basic.get("merged_at"),
                        "base_sha": pr_basic["base"]["sha"],
                        "head_sha": pr_basic["head"]["sha"],
                        "merge_commit_sha": pr_basic.get("merge_commit_sha"),
                        "labels": [item["name"] for item in pr_basic.get("labels", [])],
                    },
                    "matched_queries": record.get("matched_queries", []),
                    "base_sha": base_sha,
                    "final_sha": final_sha,
                    "changed_files": files,
                    "commit_summaries": [
                        {
                            "sha": item.get("sha"),
                            "message": item.get("commit", {}).get("message", ""),
                            "parent_shas": [parent["sha"] for parent in item.get("parents", [])],
                        }
                        for item in commits
                    ],
                    "auto_filter": {
                        "status": status,
                        "exclude_reasons": sorted(set(filter_result["exclude_reasons"])),
                        "auto_signals": filter_result["auto_signals"],
                        "has_tests_before": has_tests_before,
                        "adds_new_tests": filter_result["adds_new_tests"],
                        "touches_tests": filter_result["touches_tests"],
                        "touches_code": filter_result["touches_code"],
                        "touches_source_language": filter_result["touches_source_language"],
                        "touches_target_language": filter_result["touches_target_language"],
                        "cross_language_mapping_visible": filter_result["cross_language_mapping_visible"],
                        "snapshot_error": snapshot_error,
                    },
                    "paths": {
                        "metadata_path": str(self.metadata_file_path(instance_id).relative_to(PROJECT_ROOT)),
                        "r0_path": str(r0_path.relative_to(PROJECT_ROOT)),
                        "rn_path": str(rn_path.relative_to(PROJECT_ROOT)),
                    },
                }
                dump_json(self.metadata_file_path(instance_id), metadata)

                enrich_index[instance_id] = {
                    "instance_id": instance_id,
                    "scenario": SCENARIO,
                    "subtype": self.subtype,
                    "repo_full_name": repo["full_name"],
                    "pr_number": record["pr_number"],
                    "pr_url": pr_basic.get("html_url"),
                    "title": pr_basic.get("title", ""),
                    "matched_queries": record.get("matched_queries", []),
                    "auto_status": status,
                    "auto_exclude_reasons": sorted(set(filter_result["exclude_reasons"])),
                    "auto_signals": filter_result["auto_signals"],
                    "repo_created_at": repo.get("created_at"),
                    "repo_stars": repo.get("stargazers_count"),
                    "pr_created_at": pr_basic.get("created_at"),
                    "has_tests_before": has_tests_before,
                    "adds_new_tests": filter_result["adds_new_tests"],
                    "metadata_path": str(self.metadata_file_path(instance_id).relative_to(PROJECT_ROOT)),
                    "r0_path": str(r0_path.relative_to(PROJECT_ROOT)),
                    "rn_path": str(rn_path.relative_to(PROJECT_ROOT)),
                    "base_sha": base_sha,
                    "final_sha": final_sha,
                    "enrich_error": snapshot_error,
                }
                self.save_enrich_index(enrich_index)
                processed_ids.add(instance_id)
                checkpoint["processed_instance_ids"] = sorted(processed_ids)
                self.save_checkpoint(self.enrich_checkpoint_path(), checkpoint)
                self.logger.info("Enriched %s (%s)", instance_id, status)
            except Exception as exc:  # noqa: BLE001
                diagnostics = self.collect_github_network_diagnostics()
                self.logger.error("Network diagnostics for %s: %s", instance_id, json.dumps(diagnostics, ensure_ascii=False))
                enrich_index[instance_id] = {
                    "instance_id": instance_id,
                    "scenario": SCENARIO,
                    "subtype": self.subtype,
                    "repo_full_name": record["repo_full_name"],
                    "pr_number": record["pr_number"],
                    "pr_url": record["pr_url"],
                    "title": record.get("title", ""),
                    "matched_queries": record.get("matched_queries", []),
                    "auto_status": "error",
                    "auto_exclude_reasons": [],
                    "auto_signals": [],
                    "has_tests_before": False,
                    "adds_new_tests": False,
                    "metadata_path": "",
                    "r0_path": "",
                    "rn_path": "",
                    "base_sha": "",
                    "final_sha": "",
                    "enrich_error": truncate_text(str(exc), width=300),
                    "network_diagnostics": diagnostics,
                }
                self.save_enrich_index(enrich_index)
                self.logger.exception("Failed to enrich %s: %s", instance_id, exc)

        self.logger.info("Enrich stage finished. Records: %s", len(enrich_index))

    def export_review_stage(self) -> None:
        self.logger.info("Starting export-review stage for %s", self.subtype)
        enrich_index = self.load_enrich_index()
        rows = []
        for item in sorted(enrich_index.values(), key=lambda row: (row["repo_full_name"], row["pr_number"])):
            if item.get("auto_status") != "candidate":
                continue
            metadata = load_json(PROJECT_ROOT / item["metadata_path"])
            defaults = default_languages(self.subtype)
            rows.append(
                {
                    "instance_id": item["instance_id"],
                    "scenario": SCENARIO,
                    "subtype": self.subtype,
                    "subtype_zh": SUBTYPE_ZH.get(self.subtype, self.subtype),
                    "migration_type": infer_migration_type(self.subtype),
                    "migration_type_zh": translate_single("migration_type", infer_migration_type(self.subtype)),
                    "implementation_scope": default_implementation_scope(self.subtype),
                    "implementation_scope_zh": (
                        translate_single("implementation_scope", default_implementation_scope(self.subtype))
                        or "部分功能迁移=partial_feature_migration；整仓翻译=full_repo_translation"
                    ),
                    "logic_equivalence_scope": default_logic_equivalence_scope(self.subtype),
                    "logic_equivalence_scope_zh": (
                        translate_single("logic_equivalence_scope", default_logic_equivalence_scope(self.subtype))
                        or "同一逻辑翻译=same_logic_translation；部分逻辑替换=partial_logic_replacement；逻辑不清=unclear_logic_mapping"
                    ),
                    "repo_full_name": item["repo_full_name"],
                    "repo_created_at": metadata["repo"].get("created_at", ""),
                    "repo_stars": metadata["repo"].get("stars", ""),
                    "pr_number": item["pr_number"],
                    "pr_url": item["pr_url"],
                    "pr_created_at": metadata["pull_request"].get("created_at", ""),
                    "title": item.get("title", ""),
                    "body_summary": truncate_text(metadata["pull_request"].get("body", ""), width=300),
                    "changed_file_summary": "; ".join(file["filename"] for file in metadata["changed_files"][:20]),
                    "has_tests_before": str(bool(item.get("has_tests_before", False))).lower(),
                    "has_tests_before_zh": translate_bool(item.get("has_tests_before", False)),
                    "adds_new_tests": str(bool(item.get("adds_new_tests", False))).lower(),
                    "adds_new_tests_zh": translate_bool(item.get("adds_new_tests", False)),
                    "auto_signals": "; ".join(item.get("auto_signals", [])),
                    "auto_signals_zh": translate_multi("auto_signal", "; ".join(item.get("auto_signals", []))),
                    "manual_label": "",
                    "manual_label_zh": "正例=positive；负例=negative；不确定=uncertain",
                    "source_language": defaults["source_language"],
                    "source_language_zh": "",
                    "target_language": defaults["target_language"],
                    "target_language_zh": "",
                    "source_version": defaults["source_version"],
                    "target_version": defaults["target_version"],
                    "migration_pattern": "",
                    "test_framework": infer_test_framework(
                        [file["filename"] for file in metadata["changed_files"]],
                        metadata["pull_request"].get("body", ""),
                    ),
                    "test_framework_zh": "",
                    "build_system": infer_build_system(
                        [file["filename"] for file in metadata["changed_files"]],
                        metadata["pull_request"].get("body", ""),
                    ),
                    "build_system_zh": "",
                    "reproducible": "unknown",
                    "reproducible_zh": translate_single("reproducible", "unknown"),
                    "issue_rewrite_ready": "needs_check",
                    "issue_rewrite_ready_zh": translate_single("issue_rewrite_ready", "needs_check"),
                    "leakage_risk": infer_leakage_risk(
                        metadata["pull_request"].get("title", ""),
                        metadata["pull_request"].get("body", ""),
                    ),
                    "leakage_risk_zh": "",
                    "exclude_reason": "",
                    "exclude_reason_zh": "",
                    "reviewer": "",
                    "cross_check_status": "",
                    "cross_check_status_zh": "待复核=pending；已复核=checked；有分歧=disagreed",
                    "notes": "",
                }
            )

            rows[-1]["source_language_zh"] = translate_single("language", rows[-1]["source_language"])
            rows[-1]["target_language_zh"] = translate_single("language", rows[-1]["target_language"])
            rows[-1]["test_framework_zh"] = translate_single("test_framework", rows[-1]["test_framework"])
            rows[-1]["build_system_zh"] = translate_single("build_system", rows[-1]["build_system"])
            rows[-1]["leakage_risk_zh"] = translate_single("leakage_risk", rows[-1]["leakage_risk"])

        write_csv_rows(self.review_csv_path(), rows, REVIEW_FIELDS)
        write_review_display_csv(self.review_display_csv_path(), rows, REVIEW_FIELDS)
        self.write_cross_check_sample(rows)
        self.logger.info("Review CSV exported: %s rows", len(rows))

    def write_cross_check_sample(self, rows: List[Dict[str, Any]]) -> None:
        sample_path = REVIEW_DIR / "cross_check_sample.csv"
        if not rows:
            write_csv_rows(sample_path, [], ["subtype", "instance_id", "repo_full_name", "pr_number", "pr_url"])
            return
        sample_size = max(1, round(len(rows) * 0.1))
        sample = random.sample(rows, k=min(sample_size, len(rows)))
        sample_rows = [
            {
                "subtype": row["subtype"],
                "instance_id": row["instance_id"],
                "repo_full_name": row["repo_full_name"],
                "pr_number": row["pr_number"],
                "pr_url": row["pr_url"],
            }
            for row in sample
        ]
        write_csv_rows(sample_path, sample_rows, ["subtype", "instance_id", "repo_full_name", "pr_number", "pr_url"])

    def validate_review_schema(self, rows: List[Dict[str, str]]) -> None:
        expected_fields = self.review_schema["fields"]
        if not rows:
            return
        actual_fields = list(rows[0].keys())
        if actual_fields != expected_fields:
            raise ValueError(f"Review CSV headers do not match schema.\nExpected: {expected_fields}\nActual:   {actual_fields}")

    def build_processed_record(self, review_row: Dict[str, str], metadata: Dict[str, Any]) -> Dict[str, Any]:
        defaults = default_languages(self.subtype)
        manual_label = (review_row.get("manual_label") or "").strip().lower()
        summary = truncate_text(
            f"{metadata['pull_request'].get('title', '')}. {metadata['pull_request'].get('body', '')}",
            width=280,
        )
        return {
            "instance_id": review_row["instance_id"],
            "scenario": SCENARIO,
            "subtype": self.subtype,
            "migration_type": review_row.get("migration_type") or infer_migration_type(self.subtype),
            "implementation_scope": review_row.get("implementation_scope") or default_implementation_scope(self.subtype),
            "logic_equivalence_scope": review_row.get("logic_equivalence_scope") or default_logic_equivalence_scope(self.subtype),
            "repo": metadata["repo"]["full_name"],
            "repo_created_at": metadata["repo"].get("created_at", ""),
            "repo_stars": metadata["repo"].get("stars", ""),
            "pr_number": metadata["pr_number"],
            "pr_url": metadata["pr_url"],
            "pr_created_at": metadata["pull_request"].get("created_at", ""),
            "base_sha": metadata["base_sha"],
            "final_sha": metadata["final_sha"],
            "source_language": review_row.get("source_language") or defaults["source_language"],
            "target_language": review_row.get("target_language") or defaults["target_language"],
            "source_version": review_row.get("source_version") or defaults["source_version"],
            "target_version": review_row.get("target_version") or defaults["target_version"],
            "migration_pattern": review_row.get("migration_pattern", ""),
            "summary": summary,
            "has_tests_before": metadata["auto_filter"]["has_tests_before"],
            "adds_new_tests": metadata["auto_filter"]["adds_new_tests"],
            "r0_path": metadata["paths"]["r0_path"],
            "rn_path": metadata["paths"]["rn_path"],
            "metadata_path": metadata["paths"]["metadata_path"],
            "reviewer": review_row.get("reviewer", ""),
            "manual_label": manual_label,
            "exclude_reason": review_row.get("exclude_reason", ""),
            "cross_check_status": review_row.get("cross_check_status", ""),
            "label_confidence": "high" if manual_label == "positive" else "medium",
            "matched_queries": metadata.get("matched_queries", []),
            "auto_signals": metadata["auto_filter"].get("auto_signals", []),
            "test_framework": review_row.get("test_framework", ""),
            "build_system": review_row.get("build_system", ""),
            "reproducible": review_row.get("reproducible", ""),
            "issue_rewrite_ready": review_row.get("issue_rewrite_ready", ""),
            "leakage_risk": review_row.get("leakage_risk", ""),
            "notes": review_row.get("notes", ""),
        }

    def apply_review_stage(self) -> None:
        self.logger.info("Starting apply-review stage for %s", self.subtype)
        review_path = Path(self.args.review_file) if self.args.review_file else self.review_csv_path()
        rows = read_csv_rows(review_path)
        self.validate_review_schema(rows)

        processed_records = []
        for row in rows:
            manual_label = (row.get("manual_label") or "").strip().lower()
            if manual_label not in {"positive", "negative", "uncertain"}:
                continue
            if manual_label == "negative":
                continue
            metadata_path = self.metadata_file_path(row["instance_id"])
            if not metadata_path.exists():
                raise FileNotFoundError(f"Metadata missing for {row['instance_id']}: {metadata_path}")
            metadata = load_json(metadata_path)
            processed_records.append(self.build_processed_record(row, metadata))

        dump_jsonl(self.processed_path(), processed_records)
        self.logger.info("Processed candidates written: %s", len(processed_records))

    def aggregate_processed_records(self, include_uncertain: bool = False) -> List[Dict[str, Any]]:
        all_records: List[Dict[str, Any]] = []
        for subtype in SUPPORTED_SUBTYPES:
            path = PROCESSED_DIR / f"{subtype}_candidates.jsonl"
            for record in load_jsonl(path):
                if record.get("manual_label") == "positive" or include_uncertain:
                    all_records.append(record)
        return sorted(all_records, key=lambda row: (row["subtype"], row["repo"], row["pr_number"]))

    def compute_stats(self, review_rows: List[Dict[str, str]]) -> Dict[str, Any]:
        collect_records = list(self.load_collect_index().values())
        enrich_records = list(self.load_enrich_index().values())
        processed_records = load_jsonl(self.processed_path())

        raw_pr_count = 0
        for query_spec in self.query_specs():
            query_dir = self.subtype_search_dir() / query_spec["name"]
            for page_file in query_dir.glob("page_*.json"):
                raw_payload = load_json(page_file)
                raw_pr_count += len(raw_payload.get("items", []))

        manual_counter = Counter((row.get("manual_label") or "").strip().lower() for row in review_rows if row.get("manual_label"))
        candidate_records = [item for item in enrich_records if item.get("auto_status") == "candidate"]
        enrich_error_records = [item for item in enrich_records if item.get("auto_status") == "error"]
        query_hits = Counter()
        auto_exclude_reasons = Counter()
        for item in collect_records:
            if item.get("collect_status") != "candidate":
                continue
            for query_name in item.get("matched_queries", []):
                query_hits[query_name] += 1
        for item in enrich_records:
            for reason in item.get("auto_exclude_reasons", []):
                auto_exclude_reasons[reason] += 1

        migration_type_counter = Counter((row.get("migration_type") or "").strip() for row in review_rows if row.get("migration_type"))
        implementation_scope_counter = Counter(
            (row.get("implementation_scope") or "").strip() for row in review_rows if row.get("implementation_scope")
        )
        logic_equivalence_counter = Counter(
            (row.get("logic_equivalence_scope") or "").strip()
            for row in review_rows
            if row.get("logic_equivalence_scope")
        )
        reproducible_counter = Counter((row.get("reproducible") or "").strip().lower() for row in review_rows if row.get("reproducible"))
        rewrite_ready_counter = Counter((row.get("issue_rewrite_ready") or "").strip().lower() for row in review_rows if row.get("issue_rewrite_ready"))

        return {
            "scenario": SCENARIO,
            "subtype": self.subtype,
            "raw_pr_count": raw_pr_count,
            "unique_pr_count": len(collect_records),
            "collect_candidate_count": sum(1 for item in collect_records if item.get("collect_status") == "candidate"),
            "auto_filtered_candidate_count": len(candidate_records),
            "auto_excluded_count": sum(1 for item in enrich_records if item.get("auto_status") == "excluded"),
            "enrich_error_count": len(enrich_error_records),
            "manual_positive_count": manual_counter.get("positive", 0),
            "manual_negative_count": manual_counter.get("negative", 0),
            "manual_uncertain_count": manual_counter.get("uncertain", 0),
            "processed_candidate_count": len(processed_records),
            "has_tests_before_ratio": round(
                sum(1 for item in candidate_records if item.get("has_tests_before")) / len(candidate_records),
                4,
            )
            if candidate_records
            else 0.0,
            "adds_new_tests_ratio": round(
                sum(1 for item in candidate_records if item.get("adds_new_tests")) / len(candidate_records),
                4,
            )
            if candidate_records
            else 0.0,
            "query_hit_rate": dict(sorted(query_hits.items())),
            "auto_exclude_reason_breakdown": dict(sorted(auto_exclude_reasons.items())),
            "migration_type_breakdown": dict(sorted(migration_type_counter.items())),
            "implementation_scope_breakdown": dict(sorted(implementation_scope_counter.items())),
            "logic_equivalence_scope_breakdown": dict(sorted(logic_equivalence_counter.items())),
            "reproducible_breakdown": dict(sorted(reproducible_counter.items())),
            "issue_rewrite_ready_breakdown": dict(sorted(rewrite_ready_counter.items())),
        }

    def package_stage(self) -> None:
        self.logger.info("Starting package stage for %s", self.subtype)
        review_path = Path(self.args.review_file) if self.args.review_file else self.review_csv_path()
        review_rows = read_csv_rows(review_path) if review_path.exists() else []
        if review_rows:
            self.validate_review_schema(review_rows)

        subtype_records = []
        for record in load_jsonl(self.processed_path()):
            if record.get("manual_label") == "positive" or self.args.include_uncertain:
                subtype_records.append(record)
        dump_jsonl(self.processed_path(), subtype_records)

        stats = self.compute_stats(review_rows)
        dump_json(self.stats_path(), stats)

        global_records = self.aggregate_processed_records(include_uncertain=self.args.include_uncertain)
        dump_jsonl(self.global_dataset_path(), global_records)
        self.logger.info(
            "Package stage finished. subtype_records=%s, global_records=%s",
            len(subtype_records),
            len(global_records),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Porta-Bench language migration data collector")
    parser.add_argument("--stage", choices=SUPPORTED_STAGES, required=True)
    parser.add_argument("--subtype", choices=SUPPORTED_SUBTYPES, required=True)
    parser.add_argument("--token-file", default=str(DEFAULT_TOKEN_FILE))
    parser.add_argument("--query", nargs="+", default="", help="Run collect with a single adhoc GitHub search query string")
    parser.add_argument("--query-file", default=str(DEFAULT_QUERY_FILE))
    parser.add_argument("--review-schema-file", default=str(DEFAULT_REVIEW_SCHEMA_FILE))
    parser.add_argument("--limits-file", default=str(DEFAULT_LIMITS_FILE))
    parser.add_argument("--review-file", default="")
    parser.add_argument("--max-prs", type=int, default=None)
    parser.add_argument("--max-search-pages", type=int, default=None)
    parser.add_argument("--sleep-sec", type=float, default=None)
    parser.add_argument("--include-uncertain", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    collector = PortaBenchCollector(args)
    collector.run()


if __name__ == "__main__":
    main()
