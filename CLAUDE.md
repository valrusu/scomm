# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`scomm` ("stream comparator") compares two **unsorted** files/streams and classifies lines as common, only-in-INPUT1 (obsolete), or only-in-INPUT2 (new). Like `comm(1)` but tolerates unsorted input and adds key/payload field matching, header skipping, and a memory-bounded batch mode. Targets very large datasets (hundreds of millions of lines).

## Build, test, run

```sh
go build ./...                      # build everything
go build -o cmd/scomm/scomm ./cmd/scomm   # build the CLI binary
go test ./...                       # run all tests
go test -run TestParseItem .        # run a single test (root pkg)
go vet ./...
```

There is no lint config beyond `go vet`/`gofmt`; keep code `gofmt`-clean.

## Code layout

- Root package `scomm` (`scomm.go`) — the library. Entry point is the exported `Scomm(...)` function.
- `cmd/scomm/main.go` — thin CLI wrapper: parses flags, calls `scomm.Scomm`. The many `cmd/scomm/*.txt`, `test*`, and binary files there are gitignored scratch I/O, not source.
- `scomm_test.go` — tests for the pure helpers (`parseItem`, `parseList`, `getCompoundFieldValue`, `percentage`).

## Architecture — read this before editing `scomm.go`

**File-descriptor I/O, not path arguments.** The tool never opens files by name. Callers wire numbered FDs from the shell: inputs on FD 3 (INPUT1/old) and FD 4 (INPUT2/new); outputs on FD 5 (only-in-INPUT1), FD 6 (only-in-INPUT2), FD 7 (common). `GetFDFile` wraps a raw FD with `os.NewFile`; usage looks like `scomm 3<old 4<new 5>old_only 6>new_only 7>common`. Process substitution (`3< <(cmd)`) works for files but not arbitrary pipes.

**Heavy package-level global state.** Counters, the in-memory maps, the scanners (`sc3`/`sc4`), the `file3..file7` handles, and all parsed options are package globals (top of `scomm.go`). `Scomm` resets the counters on entry so it can be called more than once in a process. Be careful: helpers read these globals directly rather than taking parameters.

**Six processing modes, dispatched by one `switch`** in `Scomm` on the tuple `(batchSize, useKey, fullLineOutput)`:

| batch | key? | full-line? | function |
|-------|------|-----------|----------|
| no  | no  | –   | `lineMatchLineOutput` |
| no  | yes | no  | `keyMatchPayloadOutput` |
| no  | yes | yes | `keyMatchLineOutput` |
| yes | no  | –   | `lineSearchLineOutputBatch` |
| yes | yes | no  | `keySearchPayloadOutputBatch` — **stub, returns "not implemented"** |
| yes | yes | yes | `keySearchFullOutputBatch` — **stub, returns "not implemented"** |

Non-batch modes load all of INPUT1 into a map, then stream INPUT2 against it. Batch mode (`-b`) reads both inputs in alternating chunks of `batchSize` lines so INPUT1 is never fully resident. The three map variants exist to minimize memory: `linesFile*LL` (`map[string]struct{}`, full-line compare), `linesFile*KP` (`map[string]string`, key→payload), `linesFile*KL` (`map[string]lineParts`, key→{payload,fullline}).

**Key/payload matching.** `-k`/`-p` take `cut`-style LISTs (`1`, `2-4`, `5-`, `-6`, `2,4-6`). `parseItem`/`parseList` turn these into `[][2]int` ranges; `getCompoundFieldValue` extracts them — by fixed character positions when `-d` is empty, or by delimited fields when `-d` is set. Fields/positions are **1-based**; `0` in a range bound means "open end". Two records are "same" when keys match AND payloads match; same key + different payload = an update.

**Merge vs delete/insert (`-m`).** With `-m` (default true), a changed record is emitted only on FD 6 (a merge/upsert). With `-m=false`, it is emitted on both FD 5 (delete old) and FD 6 (insert new). See README "Field-Based Comparison" for the full semantics.

**Output is concurrent.** After the compare loop, the two residual-map writers (`writeFile*Data*`) run in goroutines and are joined over a `done` channel; FD 7 (common) is written inline during the loop as matches are found.

## Gotchas

- FD mapping is fixed: **FD5 = only-INPUT1 (obsolete), FD6 = only-INPUT2 (new), FD7 = common** (`-1/-2/-3` suppress FD5/6/7 respectively).
- Map pre-sizes (`MAPSIZE`, the `make(..., N)` hints) are tuned for huge inputs and allocate aggressively; lower them if running small local experiments.
- `vrb(...)` is verbose-gated logging (`-v`); `dbg(...)` is a debug helper slated for removal — don't add new calls to it.
