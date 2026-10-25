# Security policy

## Status

Tarn is a research prototype. It has **no TLS**, takes MySQL passwords on
the command line, and its storage nodes use a single privileged user. Run it
only on trusted networks; see [docs/operations.md](docs/operations.md#security).

## Supported versions

Only the latest commit on `main` receives fixes.

## Reporting a vulnerability

Please report vulnerabilities **privately**, through GitHub's
[private vulnerability reporting](https://github.com/cstheorist/tarn/security/advisories/new),
not in public issues. Include what you found, how to reproduce it, and the
impact you expect. You should get an answer within a week.

Correctness bugs in the transaction protocol (lost or duplicated writes,
anomalies that strict serializability forbids) are treated with the same
care: report them privately if they could corrupt data, or open an issue
with a reproduction otherwise.
