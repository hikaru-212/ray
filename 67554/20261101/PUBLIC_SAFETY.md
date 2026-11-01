# Public-content safety review

## Scope

The package contains Markdown, JSON summaries and short text/JSON excerpts only.
It intentionally excludes full logs, Ray session directories, virtual
environments, native binaries, model/tokenizer files, caches, sockets and
background-process state.

## Sensitive-content checks

Before packaging was declared ready, all included regular files were searched
for common credential and secret forms, including:

- authorization/cookie/session headers;
- access/secret/API tokens and passwords;
- GitHub, AWS, Google and OpenAI token patterns;
- PEM/OpenSSH/private-key material;
- signed URL query parameters and cloud credentials.

No credential, password, authentication header, signed URL or private key was
found. HTTP excerpts contain only content metadata and fixed test request IDs.

## Machine-local provenance retained

The required byte-identical `d3b2-final-summary.original.json` retains the
generic VM username/path component `/home/researcher`, `/tmp` runtime paths,
process/object IDs and the RFC1918 VM address `192.168.64.4`. These are not
credentials or routable public endpoints. The scan found the internal hostname
`research-sandbox-work-3` only in nonessential D2/D3A raw summaries; those copies
were excluded and replaced with de-identified audit-derived summaries. No personal
name, email address or external service identity is present.

Derived excerpts remove ANSI color, PIDs where unnecessary, raw memory-address
representations and repeated noisy context. They explicitly cite the private
source path and original line number. The untouched large originals remain in
the local evidence directories listed in `SOURCE_LEDGER.md`.

## File-form audit

- No symlink, device, FIFO or socket is permitted in the package.
- No executable, binary, archive, wheel, shared object, model file or venv is included.
- No single large raw log or full snapshot file is included.
- Package size is checked against the preferred 2 MiB ceiling.
- `MANIFEST.sha256` covers every packaged regular file except itself.

## Publication boundary

This safety review covers accidental secret/PII exposure and package form. It
does not authorize publication, endorse the technical conclusion, or replace
the submitter's personal line-by-line review before commit and push.

