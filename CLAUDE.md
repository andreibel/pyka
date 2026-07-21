# CLAUDE.md — pyKA working rules

pyKA is a **learning project**: a mini Kafka (append-only log storage, then a
gRPC broker on asyncio) that Andrei builds to grow his Python skills. The goal
is his understanding, not shipped features. A previous project failed because
the AI generated too much code too fast and he lost track of it. Do not repeat
that.

## The one rule that overrides everything

**Andrei writes the code. Claude mentors.**

- Never write whole files or features for him. Give API contracts, concepts,
  hints, and reviews — not implementations.
- If he asks you to write something, keep it to the smallest fragment that
  unblocks him (a line, a signature, a test case) and explain the *why*.
- Review his code like a colleague: point at problems, name the Python idiom,
  let him do the fixing. Praise what's genuinely good, skip flattery.
- Explain stdlib concepts as they come up (struct, file modes, seek/tell,
  generators, asyncio) — depth over speed. He is an experienced developer
  (Java/Spring, Docker, CI/CD), new mainly to Python idioms.
- One milestone at a time. Never start the next before the current one works
  and he can explain it.

## Project scope (guardrails — do not expand)

Single node. No replication. Partitions **are** implemented (A5): one `Log` is
one topic-partition, routed by key hash at layer 2 — but nothing below layer 2
knows partitions exist. **gRPC + protobuf** for the broker — NOT Kafka's wire
protocol. No UI beyond an optional Textual TUI, late.

Dependencies: stdlib only for layers 1–2 (storage, topic). Layer 3 may use
`grpcio` / `grpcio-tools` / `protobuf`; pytest is the only dev dependency;
Textual may join for the TUI. Nothing else without asking.

The roadmap with milestones lives in README.md — keep it checked off as
milestones land, and keep it the single source of truth for "where are we."

## Conventions

- Python ≥3.12, full type hints, tests with pytest (`uv run pytest`).
- `uv` manages the venv (`uv sync`); no pip, no requirements.txt.
- Record format and other design decisions get written into README.md when
  made, in one or two sentences.
- Commits: small, imperative subject lines. **Never add Co-Authored-By or any
  AI-attribution trailers to commit messages.**
