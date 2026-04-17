# cpp_python 进度看板

- 更新时间: `2026-04-18 00:21:36`
- 触发节点: `export-review`

## 阶段状态

| 节点 | 状态 | 说明 |
| --- | --- | --- |
| `collect` | `done` | unique=535 |
| `enrich` | `partial` | records=28/150 |
| `export-review` | `done` | rows=15 |
| `apply-review` | `pending` | processed=0 |
| `package` | `done` | stats=yes |

## 数量概览

- Collect 候选: 150/535 [######..............] 28%
- Enrich 完成: 28/150 [####................] 19%
- Enrich 候选: 15/28 [###########.........] 54%
- 人工已标注: 0/15 [....................] 0%
- Processed 保留: 0/15 [....................] 0%

## 中间产物

- Collect 索引: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\raw\pr_metadata\cpp_python\collect_index.jsonl`
- Enrich 索引: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\raw\pr_metadata\cpp_python\enriched_index.jsonl`
- Review CSV: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\review\cpp_python_manual_review.csv`
- Processed JSONL: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\processed\cpp_python_candidates.jsonl`
- Stats JSON: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\stats\cpp_python_stats.json`

## 当前阻塞

- 暂无已记录的 enrich 错误。

## 统计快照

- raw_pr_count: `700`
- unique_pr_count: `535`
- collect_candidate_count: `150`
- auto_filtered_candidate_count: `15`
- auto_excluded_count: `13`
- enrich_error_count: `0`
- manual_positive_count: `0`
- manual_negative_count: `0`
- manual_uncertain_count: `0`
- processed_candidate_count: `0`
