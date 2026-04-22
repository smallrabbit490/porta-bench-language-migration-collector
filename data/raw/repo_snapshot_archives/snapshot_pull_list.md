# Repo Snapshot Pull List

这个目录专门存放可提交到 GitHub 的快照归档，以及让队友一键恢复的脚本。

- 更新时间: `2026-04-21T14:49:30`
- 快照总数: `1`

## 一键恢复

拉取仓库更新后，在项目根目录运行：

```powershell
powershell -ExecutionPolicy Bypass -File data/raw/repo_snapshot_archives/pull_and_restore_snapshot_archives.ps1 -All
```

如果只恢复某一种 subtype：

```powershell
powershell -ExecutionPolicy Bypass -File data/raw/repo_snapshot_archives/pull_and_restore_snapshot_archives.ps1 -Subtype py2_py3
```

如果只恢复一个具体样本：

```powershell
powershell -ExecutionPolicy Bypass -File data/raw/repo_snapshot_archives/pull_and_restore_snapshot_archives.ps1 -InstanceId py2_py3__example__repo__pr1
```

## 当前可拉取快照

| instance_id | subtype | repo | pr | archive parts | manifest |
| --- | --- | --- | --- | --- | --- |
| `py2_py3__Ananay28425__Sequence-LLM__pr3` | `py2_py3` | `Ananay28425/Sequence-LLM` | `3` | `1` | `data/raw/repo_snapshot_archives/py2_py3/py2_py3__Ananay28425__Sequence-LLM__pr3/manifest.json` |
