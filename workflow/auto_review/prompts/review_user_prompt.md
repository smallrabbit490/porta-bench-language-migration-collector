Sample evidence is provided as JSON.

Use the following judgment standard before filling fields:

- Positive cross-language migration: r0 contains source-language logic, and rn
  shows that the same feature or core behavior is now implemented, replaced, or
  clearly taken over by the target language.
- Also treat a sample as positive when the evidence shows a project or module
  rewrite/replace handoff: old source-language files or runtime path are
  removed, the target-language path becomes primary, and the PR explicitly says
  rewrite / replace / port / migrate.
- Negative: the PR only adds wrappers, bindings, SDK/client code, parser logic,
  transpilers, bridges, code generators, examples, docs, CI, tests, or support
  utilities without an actual source->target implementation migration.
- Uncertain: there is some language relation, but the evidence does not clearly
  show that an existing implementation was migrated or handed over.
- If the sample looks more like a substantial rewrite than a thin wrapper
  change, prefer `uncertain` or `positive` over automatic `negative`.
Please fill the annotation fields used by the review tool:

- manual_label
- implementation_scope
- logic_equivalence_scope
- source_language
- target_language
- source_version
- target_version
- migration_pattern
- test_framework
- build_system
- reproducible
- issue_rewrite_ready
- leakage_risk
- exclude_reason
- reviewer
- cross_check_status
- notes

Return JSON only.

`reviewer` should be `auto_gpt54`.
`cross_check_status` should default to `pending`.
`notes` must never be empty.
`notes` must contain at least two short evidence-based sentences.
`notes` should explicitly mention at least one concrete file path or concrete
code clue from the evidence JSON.
