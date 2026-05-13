# 自动评审工作流

`workflow/auto_review` 是 Porta-Bench 语言迁移候选样本的机器辅助评审工作流。
它把已经准备好快照的 PR metadata 转换成可供评审工具消费的 JSON 标注结果，
流程由证据包构建、启发式预填、LLM 评审、schema 校验和最终结果导出组成。

这个工作流本身不会抓取 GitHub PR，也不会自己生成仓库快照。它依赖前置的
collect / enrich 流程已经准备好下面这些输入：

- `data/raw/pr_metadata/<subtype>/<instance_id>.json`
- `data/raw/repo_snapshots/<subtype>/<snapshot_bundle>/r0`
- `data/raw/repo_snapshots/<subtype>/<snapshot_bundle>/rn`

支持的 subtype：

- `py2_py3`
- `cpp_python`
- `java_python`
- `python_cpp`
- `python_java`

## 目录结构

```text
workflow/auto_review/
|-- README.md
|-- run_auto_review.py
|-- cleanup_negative_snapshots.py
|-- start_remaining_auto_review.ps1
|-- workflow预览.png
|-- configs/
|   |-- pipeline_config.json
|   `-- output_schema.json
`-- prompts/
    |-- review_system_prompt.md
    `-- review_user_prompt.md
```

| 路径 | 作用 |
| --- | --- |
| `run_auto_review.py` | 主入口脚本，负责证据构建、启发式评审、LLM 评审、自检、最终合并和批量运行。 |
| `cleanup_negative_snapshots.py` | 维护辅助脚本，用于汇总标签，并在确认后删除被评为负例的 snapshot / archive 目录。建议先 dry-run。 |
| `start_remaining_auto_review.ps1` | PowerShell 后台启动脚本，用来批量跑剩余 subtype 的 auto-review。 |
| `workflow预览.png` | 本地讨论和说明时使用的流程预览图。 |
| `configs/pipeline_config.json` | 运行配置，包括模型提供方、env 文件、API key 名称、base URL / model env 名称、并发、超时和截断长度。 |
| `configs/output_schema.json` | 最终评审 JSON 的必填字段和允许的枚举值。 |
| `prompts/review_system_prompt.md` | system prompt，定义 positive / negative / uncertain 的判断策略。 |
| `prompts/review_user_prompt.md` | user prompt，描述需要填写的评审字段。 |

## 运行配置

当前默认配置位于：

`workflow/auto_review/configs/pipeline_config.json`

目前仓库里提交的默认配置使用 OpenAI-compatible 的 chat completions 接口：

```json
{
  "model_provider": "openai_compatible",
  "model_name": "gpt-5.4",
  "env_file": ".env",
  "api_key_priority": ["DEFAULT_LLM_API_KEY2"],
  "base_url_env_name": "DEFAULT_LLM_BASE_URL2",
  "model_env_name": "DEFAULT_LLM_MODEL_NAME2"
}
```

密钥不会提交到仓库。请把 key 放在仓库根目录的 `.env` 里，或者直接写到 shell
环境变量中。环境变量会覆盖 `.env` 中的同名值。

`.env` 示例：

```text
DEFAULT_LLM_API_KEY2=...
DEFAULT_LLM_BASE_URL2=https://example-compatible-endpoint/v1
DEFAULT_LLM_MODEL_NAME2=gpt-5.4
```

Python 依赖：

```powershell
pip install openai zhipuai
```

`zhipuai` 仍然保留，是为了兼容之前基于 GLM 的运行方式；当前默认 provider 是
`openai_compatible`。

## 主流程

下面的命令都默认从仓库根目录执行。

### 1. build-evidence

从 PR metadata、变更文件、patch 片段和 r0/rn 文件预览里构建压缩证据包。

```powershell
python workflow\auto_review\run_auto_review.py --stage build-evidence --instance-id py2_py3__amillb__pgMapMatch__pr34
```

输出：

```text
data/auto_review/evidence/<subtype>/<instance_id>.json
```

### 2. heuristic-review

不调用 LLM，只生成规则驱动的预填草稿。

```powershell
python workflow\auto_review\run_auto_review.py --stage heuristic-review --instance-id py2_py3__amillb__pgMapMatch__pr34
```

输出：

```text
data/auto_review/drafts/<subtype>/<instance_id>.heuristic.json
```

### 3. llm-review

调用配置好的模型，保存原始回答和解析后的 JSON。

```powershell
python workflow\auto_review\run_auto_review.py --stage llm-review --instance-id py2_py3__amillb__pgMapMatch__pr34
```

输出：

```text
data/auto_review/drafts/<subtype>/<instance_id>.llm.json
```

### 4. self-check

把 heuristic 和 LLM 字段合并，强制修正 subtype 的方向字段，校验枚举值，并根据
`output_schema.json` 检查必填项。

```powershell
python workflow\auto_review\run_auto_review.py --stage self-check --instance-id py2_py3__amillb__pgMapMatch__pr34
```

输出：

```text
data/auto_review/drafts/<subtype>/<instance_id>.checked.json
```

### 5. merge-results

写出最终 review JSON，以及一个 trace 文件，记录最终结果是由哪些中间产物合并而来。

```powershell
python workflow\auto_review\run_auto_review.py --stage merge-results --instance-id py2_py3__amillb__pgMapMatch__pr34
```

输出：

```text
data/auto_review/final/by_instance/<instance_id>.json
data/auto_review/final/traces/<instance_id>.json
```

### 单样本全流程

```powershell
python workflow\auto_review\run_auto_review.py --stage run-all --instance-id py2_py3__amillb__pgMapMatch__pr34 --attempts 3
```

### 批量运行

```powershell
python workflow\auto_review\run_auto_review.py --stage run-batch --subtype py2_py3 --limit 30 --workers 2 --attempts 3 --resume
```

批量运行汇总文件：

```text
data/auto_review/final/batch_runs/<subtype>__limit<N>.json
```

批量参数说明：

| 参数 | 含义 |
| --- | --- |
| `--limit N` | 最多处理 `N` 个 snapshot-ready 样本。`0` 表示不显式限制。 |
| `--workers N` | 并发 worker 数。 |
| `--attempts N` | 单样本失败后的重试次数。 |
| `--resume` | 如果 final 已存在，则跳过该样本。 |
| `--reviewer-filter NAME` | 只处理 `data/review_results/by_instance` 中 reviewer 匹配 `NAME` 的样本。 |
| `--write-review-results` | 额外把 final JSON 写入 `data/review_results/by_instance`。这个目录属于人工 / Web 标注结果区，确认质量后再用。 |

## 哪些阶段不需要 API key

下面这些阶段可以在没有模型权限的情况下做本地验证：

- `build-evidence`
- `heuristic-review`
- `self-check`，前提是已经有 llm draft
- `merge-results`，前提是已经有 checked draft

下面这些阶段需要模型密钥：

- `llm-review`
- `run-all`
- `run-batch`

## 维护辅助脚本

汇总 final auto-review 标签：

```powershell
python workflow\auto_review\cleanup_negative_snapshots.py --mode stats
```

dry-run 方式查看将要删除的负例 snapshot：

```powershell
python workflow\auto_review\cleanup_negative_snapshots.py --mode delete --label negative --subtype py2_py3 --limit 5
```

只有在确认 dry-run 输出没问题以后，才执行真实删除：

```powershell
python workflow\auto_review\cleanup_negative_snapshots.py --mode delete --label negative --subtype py2_py3 --limit 5 --apply
```

`delete` 模式是故意设计得比较显式的。它会删除
`data/raw/repo_snapshots` 和 `data/raw/repo_snapshot_archives` 下的目录，
并更新 snapshot list 文件。没有确认前不要直接加 `--apply`。

## 验证命令

基础 smoke test：

```powershell
python -m py_compile workflow\auto_review\run_auto_review.py workflow\auto_review\cleanup_negative_snapshots.py
python workflow\auto_review\run_auto_review.py --help
python workflow\auto_review\cleanup_negative_snapshots.py --help
```

无 API 的本地样本验证，前提是 metadata 和 snapshots 已存在：

```powershell
python workflow\auto_review\run_auto_review.py --stage build-evidence --instance-id py2_py3__amillb__pgMapMatch__pr34
python workflow\auto_review\run_auto_review.py --stage heuristic-review --instance-id py2_py3__amillb__pgMapMatch__pr34
```

`data/auto_review/` 是生成输出目录。把结果额外写入
`data/review_results/by_instance` 之前，先人工检查。

## 评审判断策略

prompts 采用的是保守判断规则：

- cross-language 正例需要有证据表明源语言实现被替换、翻译，或者功能上明确交接给目标语言实现
- wrapper、binding、bridge、client、parser、code generator、CI、文档、示例本身都不能直接算正例
- 如果证据混杂，不要硬判正例，优先给 `uncertain`
- 对 `py2_py3`，工作流会强制把方向字段固定为 `python 2 -> python 3`

最终输出字段由 `configs/output_schema.json` 约束，方便后续接入评审工具以及
`apply-review` / package 流程。
