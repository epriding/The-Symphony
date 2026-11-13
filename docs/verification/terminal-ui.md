# Terminal-ui acceptance trace

The human Textual module implements [the approved specification](../../SPEC-terminal-ui.md).
Run it using [the operating guide](../BOT_TUI.md). Tests use synthetic feeds,
local Unix sockets and actual PTYs; they do not validate public feed credentials
or real-money execution. The structured agent CLI is a separate future module.

## Reproduce

From the feature checkout with Python 3.12 and the requirements installed:

```bash
venv/bin/python -m pytest -q tests/test_bot_client.py tests/test_bot_tui.py tests/test_bot_tui_process.py
venv/bin/python -m pytest -q
venv/bin/python -m compileall -q main.py src examples tests
venv/bin/python -m pip check
git diff --check
```

Fresh smoke/replay verification uses new output directories:

```bash
tui_verify_dir=$(mktemp -d)
venv/bin/python main.py smoke --output "$tui_verify_dir/smoke"
venv/bin/python main.py replay --capture "$tui_verify_dir/smoke" --output "$tui_verify_dir/replay"
```

The actual PTY lifecycle tests invoke `main.py tui --socket` against an
independent synthetic service, start a session, drive feed barriers, exit or
kill the client, and attach again. Session identity, increasing runner revision
and risk ticks, exit codes and terminal attributes are asserted. The offline
serve/TUI path uses the same service constructor and command entrypoint; finite
smoke alone would not prove lifetime independence.

## Acceptance mapping

Names below are tests in [the client suite](../../tests/test_bot_client.py),
[the terminal suite](../../tests/test_bot_tui.py) or
[the actual-process suite](../../tests/test_bot_tui_process.py).

| ID | Passing behavior evidence |
|---|---|
| TU01 | `test_client_discovers_idle_service_without_creating_a_session`, `test_idle_tui_is_inert_and_quit_only_detaches`, `test_missing_service_stays_visible_until_operator_quits` |
| TU02 | `test_unavailable_overview_counts_display_unknown_instead_of_zero`, `test_strategy_pages_and_disabled_modes_remain_visible`, `test_strategy_detail_matches_real_fills_and_renders_findings_literally`, `test_confirmed_start_pause_resume_stop_use_real_session_controls` |
| TU03 | `test_strategy_detail_matches_real_fills_and_renders_findings_literally`, `test_large_recent_findings_stay_bounded_and_disclose_eviction` |
| TU04 | `test_confirmed_start_pause_resume_stop_use_real_session_controls`, `test_cancelled_and_replaced_confirmation_never_starts_again`, `test_identity_confirmation_and_control_notices_are_sanitized_without_retargeting` |
| TU05 | `test_lost_control_reply_stays_unknown_and_is_never_retried`, `test_late_collection_reply_cannot_replace_a_new_tab`, `test_reconnect_announces_replacement_service_without_starting_it`, `test_close_during_connect_cannot_send_a_late_mutation` |
| TU06 | `test_poll_cadence_does_not_drift_by_response_latency`, `test_stale_age_changes_while_an_overview_read_is_blocked`, `test_reconnect_announces_replacement_service_without_starting_it` |
| TU07 | `test_blocked_observation_does_not_block_confirmed_stop_or_detach`, `test_client_rejects_invalid_replies_without_retry`, `test_fragmented_large_finding_page_releases_all_payload_reservations`, `test_request_aliases_cannot_amplify_allocation_before_validation`, `test_changed_session_with_malformed_error_is_a_structured_failure` |
| TU08 | `test_actual_terminal_detach_keeps_same_bot_processing_inputs_and_risk` (q, Ctrl-C and kill variants) |
| TU09 | `test_small_terminal_requires_resize_and_restores_a_usable_layout`, `test_monochrome_ascii_display_keeps_original_strategy_identity`, `test_identity_confirmation_and_control_notices_are_sanitized_without_retargeting`, `test_actual_terminal_resize_restores_body_and_modes`, `test_actual_terminal_ascii_monochrome_output_and_quit`, `test_actual_terminal_unrecoverable_client_failure_restores_modes` plus TU03/TU04 navigation/input |
| TU10 | Full regression, compileall, pip check, fresh smoke/replay and diff checks recorded in the local canonical trail |

## Results and limits

The pre-review implementation passed 517 tests. Independent GPT-6 Astra review
at `f1a8071` reproduced two required fixes: malformed error containers on
changed-session replies and unsanitized identity/target display. Parent changes
added three malformed-container cases and a combined hostile-display/exact-target
case; their initial failures were retained before fixes. The final full suite
passed **521 tests in 163.99 seconds**, with no failures, errors or skips.
Compilation, pip check, fresh smoke/replay, local documentation links and the
documented real-PTY serve/TUI path passed. Review fixes are committed at
`ea5d2b0`.

The client memory fixture reserved 2,346,742 bytes: 294,164 owned model bytes,
217,570 wire high-water bytes and a 1,835,008-byte stream/decode allowance.
This is below the approved cooperative 8 MiB budget. It is not whole-process
RSS measurement or a measurement of simultaneous two-connection peaks.
Independent read/control responsiveness has separate blocked-read tests.

A temporary mutation removing the post-connect closure guard made its regression
test fail; restoring the exact source bytes made it pass. Long diagnostic text
also exposed a quadratic shared credential-redaction pattern. The replacement
preserves existing redaction cases and matched 10,000 deterministic synthetic
legacy examples, with a subprocess regression for long plain tokens.

Several synthetic test/benchmark storage and callback deadlines were increased
explicitly to avoid measuring host scheduling under allocation tracing. The
accounting, retention memory and dedicated deadline assertions remain in place;
production deadline defaults were not changed. These adjustments are recorded
as separate commits and rows, not described as unchanged thresholds.

The canonical append-only trail is `.audit/trading-bot-tui.tsv` in this
worktree (locally ignored). Detailed logs, JUnit output, mutation hashes and
process evidence stay alongside it. Key completion artifacts are
`tu08-regression.log`, `tu08-regression.xml`, `tu08-compile.log`,
`tu08-pip-check.log`, `tu08-smoke.log`, `tu08-replay.log`,
`tu08-review-red.log` and `tui-review-final.md`.

Review was fresh-context and read-only on GPT-6 Astra, with targeted
reproductions and an audit of then-existing evidence, not an independent
full-suite rerun. It is a different model within the same family. Cross-family
review was unavailable; no additional provider was installed. The parent
reviewed and tested subsequent fixes. Full host-transcript audit was unavailable;
the trail was checked against available conversation and artifacts. Synthetic
PTY checks establish the tested terminal behavior, not every terminal emulator
or a human visual usability review.

## Readability follow-up

The subsequent [readability verification](terminal-ui-readability.md) records
the approved diagnostic layout, visible navigation and finding-filter changes.
The results above describe the original build before those changes.
