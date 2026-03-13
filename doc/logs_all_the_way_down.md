# Logs All the Way Down

A note for future revisiting.

## The Observation

We are using git — a log-structured store — to record our work on a system
whose central insight is that the log is the database. The git commit history
is itself a temporal record of the project's evolution, and `git checkout` is
time travel in exactly the sense that DuckLake's `AS OF` is time travel.

## Git as a Tree of Logs

Git is not a single log. It is a DAG — a tree with many logs, where each
branch represents an independent timeline and each merge commit records a
causal unification of two timelines into one.

This parallels the CDC adapter design. In SQL Server CDC, a shared LSN across
multiple tables means that changes to those tables happened in the same
transaction — a single causal point spanning multiple entities. That is
structurally the same as a git merge: independent change streams unified at a
commit that records both parents.

## The Analogy Extends

| Git concept | Rule4 concept |
|---|---|
| Commit | DuckLake snapshot |
| Working tree | "The database" — optimized access path to HEAD |
| `git log` | `SELECT * FROM ducklake_snapshot ORDER BY snapshot_id` |
| `git checkout <sha>` | `AS OF` time travel |
| Merge commit | CDC snapshot with shared LSN across tables |
| Branch | Independent source change stream (Socrata, CDC, CT) |
| `git blame` | Provenance in `ducklake_snapshot_changes.commit_extra_info` |

Whether this analogy is deep or merely structural is worth revisiting once the
CDC adapter is working and we can see whether the merge semantics actually
compose the same way.
