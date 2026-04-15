# py2_py3 进度看板

- 更新时间: `2026-04-14 23:24:35`
- 触发节点: `package`

## 阶段状态

| 节点 | 状态 | 说明 |
| --- | --- | --- |
| `collect` | `done` | unique=475 |
| `enrich` | `done` | records=28/28 |
| `export-review` | `done` | rows=11 |
| `apply-review` | `pending` | processed=0 |
| `package` | `done` | stats=yes |

## 数量概览

- Collect 候选: 28/475 [#...................] 6%
- Enrich 完成: 28/28 [####################] 100%
- Enrich 候选: 11/28 [########............] 39%
- 人工已标注: 0/11 [....................] 0%
- Processed 保留: 0/11 [....................] 0%

## 中间产物

- Collect 索引: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\raw\pr_metadata\py2_py3\collect_index.jsonl`
- Enrich 索引: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\raw\pr_metadata\py2_py3\enriched_index.jsonl`
- Review CSV: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\review\py2_py3_manual_review.csv`
- Processed JSONL: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\processed\py2_py3_candidates.jsonl`
- Stats JSON: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\stats\py2_py3_stats.json`

## 当前阻塞

- 暂无已记录的 enrich 错误。

## 统计快照

- raw_pr_count: `200`
- unique_pr_count: `475`
- collect_candidate_count: `28`
- auto_filtered_candidate_count: `11`
- auto_excluded_count: `17`
- enrich_error_count: `0`
- manual_positive_count: `0`
- manual_negative_count: `0`
- manual_uncertain_count: `0`
- processed_candidate_count: `0`
