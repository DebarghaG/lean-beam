# Draft Lean declarations

Turn the requested claim into Lean declarations in the project's actual context. By default, return
a declaration with `by sorry`, its assumptions, and the result of checking it. Attempt a proof when
asked. Writing a draft to source is separate from checking it.

## Choose the statement and context

- Use the user's claim, named theorem, or supplied source. If a source contains several claims and
  the intended one is unclear, ask which to draft. Read the source before attributing a claim to it.
- Reuse the project's definitions and conventions. State encoding choices and extra assumptions;
  do not silently weaken a conclusion, add hypotheses to make a proof work, or change an existing
  declaration's signature. Raise an apparent counterexample or inconsistent specification.
- Default to returning Lean text in the reply. Edit files only when requested. For an existing file,
  preserve unrelated declarations and docstrings; a request to add a theorem does not authorize
  replacing the file.
- Find a saved module with the needed imports and a top-level position in the intended namespace
  and section. Inspect nearby declarations before choosing the position. If no suitable project
  context is available, return an explicitly unchecked draft with the missing setup identified.

For a requested new file, save and sync its intended header first so probes use its real imports.

Search the project and dependency sources with `rg`, and use `lean-beam workspace-symbols`
(`lean_workspace_symbols`) when useful. Check candidate names with `hover` / `lean_hover` or a
speculative `#check`. The upstream `lean_local_search`, `lean_leanfinder`, and `lean_multi_attempt`
tools are unavailable in Beam. Do not assume Mathlib is installed.

Follow nearby files for imports, module headers, and exported declarations. A dependency imported
only for a proof may have different visibility requirements from one used in a public signature.
Imports belong in the saved module header; a speculative declaration uses the imports already
loaded at its selected position.

## Check a skeleton

For CLI use, keep one `lean-beam --root ROOT serve` owner running. MCP owns its session itself.
Use `sync` / `lean_sync` to establish diagnostics and obtain the current version. MCP document calls include
`workspace: {"root": ROOT}` and `path`; positions are zero-based UTF-16 coordinates.

Submit one complete declaration through `run-at` / `lean_run_at` at a top-level command position.
For example, after selecting a blank line following the imports:

```bash
lean-beam --root "$root" run-at "$file" "$version" "$line" 0 --stdin <<'LEAN'
theorem add_swap (a b : Nat) : a + b = b + a := by
  sorry
LEAN
```

The matching MCP call is `lean_run_at` with `workspace`, `path`, `version`, `line`, `character: 0`,
and the declaration as `text`. Use a fresh name that does not collide with the selected context.

Check the CLI exit status and `ok` before reading `result.success`; for MCP, inspect transport errors,
`isError`, and `structuredContent.success`. A completed request can still fail inside Lean.
Read its messages and fix ordinary type, name, or instance errors. An expected sorry warning is
compatible with a checked skeleton. `success: true` establishes elaboration, not a completed proof
or agreement with the informal claim.

One command request accepts one Lean command. For dependent declarations, create the first with
`run-at-handle` / `lean_run_at_handle`, then pass its handle to `run-with-linear` /
`lean_run_with_linear` for each following declaration or `#check`. Carry the returned handle forward
and release the final live handle with `release` / `lean_release`. CLI returns `result.handle`;
MCP returns `structuredContent.next_handle`. CLI continuations accept `--handle-file PATH`.
Independent `run-at` calls always start from the saved document, so they cannot see earlier drafts.

If a declaration uses a sorry-stubbed helper, record that dependency as an outstanding proof
obligation even when the declaration's own proof succeeds. Use a continuation `#print axioms name`
when needed to check whether an apparently completed declaration still depends on `sorryAx`.

## Try a proof when requested

Start with a relevant existing lemma or a small number of plausible tactics. Keep this a short
attempt; leave the unresolved skeleton and explain the blocker if it needs substantial proof search.

For a draft that has not been written to source, resubmit the complete declaration with each
candidate proof at the same command position. Independent candidates may run concurrently.
For candidates that need earlier speculative declarations, retain the command handle from before
the candidate declaration and branch with non-consuming `run-with`; release each child afterward.
A handle returned after elaborating `theorem ... := by sorry` retains the resulting command
environment; it is not a handle to the theorem's open tactic goal.

For a skeleton already saved in a file, use `goals before` / `lean_goals` with `mode: "before"` at
the first character of `sorry`, then try a replacement tactic with `run-at` / `lean_run_at`.
Only use tactic handles when continuing a partially solved tactic state. Branch with `run-with`,
advance a single branch with `run-with-linear`, and release unused handles. Do not reuse a consumed
linear handle.

A tactic result with remaining goals is partial progress. Before reporting a completed proof,
check the full declaration without sorries, including any drafted helpers it depends on. Keep
mathematical correspondence, successful elaboration, and proof completion distinct in the report.

## Deliver or write the draft

Return the Lean declaration, required imports and context, significant formalization choices, and
remaining proof obligations. Say whether it was checked as a skeleton, checked with a completed
proof, or left unchecked with the specific reason. Do not turn a failed check into a success claim.
If Lean reports unexpected stale state or rebuild trouble, stop probing and surface it explicitly.

When the user requested a file edit, write the accepted source with the client's editor, then
`sync` / `lean_sync` the saved file. Include required imports and inspect the resulting diagnostics;
use `todo` / `lean_todo` to locate remaining sorries. A successful speculative request or `sync`
does not write the draft into the file. `save` / `lean_save` writes build artifacts and is optional
for a requested module checkpoint. A skeleton with sorries remains a draft even if it checkpoints.
Follow the base skill's build policy only when claiming batch validation; drafting itself does not
require a full project build. Drafting alone does not stage or commit changes.

Adapted from [lean4:draft](https://github.com/cameronfreer/lean4-skills/blob/362827ce684c0b4671254b5da1539b34145eb06f/plugins/lean4/commands/draft.md).
The upstream [MIT license](draft.LICENSE) is retained alongside this adaptation.
