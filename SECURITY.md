# Security policy

## Status

Tarn is a research prototype. Its development defaults permit plaintext,
optional SQL authentication, argv credentials and a privileged storage user;
keep those defaults on trusted development networks. The explicit
[production profile](docs/production.md) requires verified SQL/storage TLS,
private-file credentials, restricted storage accounts and deployment checks.
Its physical placement, clock and capacity assumptions still require operator
validation; the profile is not a general production-readiness certification.

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
