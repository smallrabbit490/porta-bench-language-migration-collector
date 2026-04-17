# python_java 进度看板

- 更新时间: `2026-04-18 00:43:09`
- 触发节点: `export-review`

## 阶段状态

| 节点 | 状态 | 说明 |
| --- | --- | --- |
| `collect` | `done` | unique=723 |
| `enrich` | `partial` | records=5/150 |
| `export-review` | `done` | rows=2 |
| `apply-review` | `pending` | processed=0 |
| `package` | `done` | stats=yes |

## 数量概览

- Collect 候选: 150/723 [####................] 21%
- Enrich 完成: 5/150 [#...................] 3%
- Enrich 候选: 2/5 [########............] 40%
- 人工已标注: 0/2 [....................] 0%
- Processed 保留: 0/2 [....................] 0%

## 中间产物

- Collect 索引: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\raw\pr_metadata\python_java\collect_index.jsonl`
- Enrich 索引: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\raw\pr_metadata\python_java\enriched_index.jsonl`
- Review CSV: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\review\python_java_manual_review.csv`
- Processed JSONL: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\processed\python_java_candidates.jsonl`
- Stats JSON: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\stats\python_java_stats.json`

## 当前阻塞

- 暂无已记录的 enrich 错误。

## 统计快照

- raw_pr_count: `800`
- unique_pr_count: `723`
- collect_candidate_count: `150`
- auto_filtered_candidate_count: `2`
- auto_excluded_count: `3`
- enrich_error_count: `0`
- manual_positive_count: `0`
- manual_negative_count: `0`
- manual_uncertain_count: `0`
- processed_candidate_count: `0`
