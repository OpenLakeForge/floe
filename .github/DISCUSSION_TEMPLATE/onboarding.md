---
name: Onboarding / Codebase tour
about: Start here if you're new to Floe and want a guided overview
title: "[Onboarding] Introduce yourself + questions"
labels: []
---

Welcome! This thread is for newcomers to share context and ask questions.

## Quick intro
- Background:
- What you want to contribute to:
- Any constraints (time/stack):

## Codebase map (short)
- `src/floe`: the ingestion engine and the `floe` CLI (`src/floe/_cli.py`)
- `docs/`: user docs and support matrix
- `example/`: sample configs + inputs

## Architecture highlights
- Storage abstraction: `src/floe/storage`
- Input formats: `src/floe/read`
- Output sinks: `src/floe/write`
- Run pipeline: `src/floe/run`
- Reports: `src/floe/report`

## How to get started
- Find a `good first issue`
- Ask questions here if the scope or design is unclear
- Check `CONTRIBUTING.md` for testing expectations

## Questions
Ask anything—happy to help.
