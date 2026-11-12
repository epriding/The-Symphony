# ADR-002: Version detail exports with per-stream sequence numbers

## Status and date

Accepted, 2026-11-11. The user authorized this archive/replay prerequisite after
reviewing a failing stress capture; it is separate from the new control API.

## Context

A full regression found matching execution/accounting data but different history
hashes after replay. Replaying the saved capture with unchanged main code also
reproduced the difference. `accounting_order` in history export schema 1 came
from the global SQLite records row ID, including unrelated streams. Different
archive flush interleavings changed those numbers without changing trade or
correction data. Retrying until the stress test passed would hide the defect.

## Decision

History export schema 2 defines `accounting_order` as the one-based ordinal
within each detail stream. The stream counter and appended row share the same
SQLite transaction, so rollback and retry do not consume an ordinal. Global
SQLite sequence keys remain unchanged for internal ordered archive paging.
Account calculations, correction fields and causal inputs are unchanged.

Capture input schemas remain readable; replay emits the current export format.
Legacy schema-1 history hashes are not byte-identical promises for schema-2
output. Source captures/artifacts stay read-only. The version distinction is
explicit in the history summary and database metadata.

## Verification and consequences

[The regression tests](../../tests/test_bot_history_order.py) exercise unrelated
stream interleavings and reconstruct the preserved failed capture at different
replay pacing. Every original trade/correction field is checked, with only the
explicitly versioned ordinal converted to its new definition. New-format history
references are compared strictly, including complete detail hashes. Existing
postcommit retry and stress tests remain enabled.

Consumers must not compare ordinals from different streams as a global clock.
They identify the exported format by schema version. No automatic archive
migration, capture rewrite, deletion or account resume is performed.
