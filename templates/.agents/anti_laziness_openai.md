# Strict Anti-Laziness & Completeness Protocol

You MUST obey every rule below without exception. Violations are caught automatically by 0-token guards and strict reviewers and result in immediate rejection.

1. **Absolute Ban on Placeholders & Abbreviations:**
   - NEVER write `// ... rest of code`, `// ... existing code`, `# ... unchanged`, `TODO`, `FIXME`, `NotImplementedError`, empty `pass` stubs, omitted imports, or comments like "similarly for remaining fields".
   - Always emit complete, production-ready implementations for every function, branch, and type.

2. **No Silent Simplification of Requirements:**
   - NEVER silently drop acceptance criteria, skip error handling, omit validation, ignore edge cases, or replace required logic with hardcoded/mocked responses.
   - If a requirement truly conflicts with the shared architecture, stop and emit `STATUS: NEEDS_ARCH_CHANGE` instead of silently downgrading the solution.

3. **Mandatory Read-Before-Edit:**
   - NEVER guess or invent file paths, module names, function signatures, class attributes, or configuration keys.
   - Always inspect the provided `REPO_MAP.md`, `context_files`, and actual repository files before editing or referencing any symbol.

4. **No False Claims of Verification:**
   - NEVER claim tests, linters, typecheckers, or builds passed unless you actually executed them in this session and observed exit code `0`.
   - Report the exact commands executed and honest outcomes in `BEGIN_REPORT`.

5. **Substantive Closure of Every Review Finding (with `file:line`):**
   - NEVER apply cosmetic patches or suppressions (`# type: ignore`, `# noqa`, `eslint-disable`) to hide reviewer or linter complaints.
   - Every reviewer remark and gate error must be fixed at the root cause and explicitly accounted for in `BEGIN_REPORT` (persisted by the orchestrator as `last_report.md`) with exact `file:line` citations.

6. **Completeness Rule for Large Files:**
   - If an implementation is large, decompose it cleanly into cohesive modules inside `allowed_paths`.
   - NEVER truncate, abbreviate, or partially implement a file to save output tokens.
