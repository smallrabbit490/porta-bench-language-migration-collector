# py2_py3 进度看板

- 更新时间: `2026-04-18 00:43:06`
- 触发节点: `export-review`

## 阶段状态

| 节点 | 状态 | 说明 |
| --- | --- | --- |
| `collect` | `done` | unique=844 |
| `enrich` | `partial` | records=38/150 |
| `export-review` | `done` | rows=18 |
| `apply-review` | `pending` | processed=0 |
| `package` | `done` | stats=yes |

## 数量概览

- Collect 候选: 150/844 [####................] 18%
- Enrich 完成: 38/150 [#####...............] 25%
- Enrich 候选: 18/38 [#########...........] 47%
- 人工已标注: 0/18 [....................] 0%
- Processed 保留: 0/18 [....................] 0%

## 中间产物

- Collect 索引: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\raw\pr_metadata\py2_py3\collect_index.jsonl`
- Enrich 索引: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\raw\pr_metadata\py2_py3\enriched_index.jsonl`
- Review CSV: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\review\py2_py3_manual_review.csv`
- Processed JSONL: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\processed\py2_py3_candidates.jsonl`
- Stats JSON: `D:\thecourceofdasi\TeacherWangWork\SWEbench（zzc）\data\stats\py2_py3_stats.json`

## 当前阻塞

- `py2_py3__Cloud-CV__EvalAI__pr4872`: git checkout d4ce4fa3125fa8ad1a9233c60f1e937fba43a4b6 failed: error: RPC failed; curl 56 schannel: server closed abruptly (missing close_notify) error: 550 bytes of body are still expected fetch-pack: unexpected disconnect while reading sideband packet fatal: early EOF fatal: fetch-pack: invalid...
- `py2_py3__LibrePhotos__librephotos__pr1780`: git checkout 0570c1d77f9af6142067d40908f02ff94d17fe95 failed:

## 统计快照

- raw_pr_count: `700`
- unique_pr_count: `844`
- collect_candidate_count: `150`
- auto_filtered_candidate_count: `18`
- auto_excluded_count: `18`
- enrich_error_count: `2`
- manual_positive_count: `0`
- manual_negative_count: `0`
- manual_uncertain_count: `0`
- processed_candidate_count: `0`
