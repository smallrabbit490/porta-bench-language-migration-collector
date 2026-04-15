# java_python 进度看板

- 更新时间: `2026-04-15 00:32:17`
- 触发节点: `package`

## 阶段状态

| 节点 | 状态 | 说明 |
| --- | --- | --- |
| `collect` | `done` | unique=32 |
| `enrich` | `done` | records=5/5 |
| `export-review` | `done` | rows=3 |
| `apply-review` | `pending` | processed=0 |
| `package` | `done` | stats=yes |

## 数量概览

- Collect 候选: 5/32 [###.................] 16%
- Enrich 完成: 5/5 [####################] 100%
- Enrich 候选: 3/5 [############........] 60%
- 人工已标注: 0/3 [....................] 0%
- Processed 保留: 0/3 [....................] 0%

## 中间产物

- Collect 索引: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\raw\pr_metadata\java_python\collect_index.jsonl`
- Enrich 索引: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\raw\pr_metadata\java_python\enriched_index.jsonl`
- Review CSV: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\review\java_python_manual_review.csv`
- Processed JSONL: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\processed\java_python_candidates.jsonl`
- Stats JSON: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\stats\java_python_stats.json`

## 当前阻塞

- 暂无已记录的 enrich 错误。

## 统计快照

- raw_pr_count: `100`
- unique_pr_count: `32`
- collect_candidate_count: `5`
- auto_filtered_candidate_count: `3`
- auto_excluded_count: `2`
- enrich_error_count: `0`
- manual_positive_count: `0`
- manual_negative_count: `0`
- manual_uncertain_count: `0`
- processed_candidate_count: `0`
