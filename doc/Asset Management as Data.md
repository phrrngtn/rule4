# Asset Management as Data: {Resources × Principals × Services} via Schema-as-Data + Lenses

## Thesis

Entitlement management — *who* (principals) may do *what* (services/verbs) on *which*
(resources) — is the hard core of security: the `R × P × S` cartesian product is
astronomical, and it has to be **correct**. The reason it reads as ETOOBIG is that people
imagine *materialising* the product. You never do. Two moves dissolve it, and both are
machinery this codebase already has:

1. **Represent the intent as data, intensionally** — name subsets by predicate/membership
   (rules), never by enumeration. The product is sparse and structured; you store the
   *rules* and *decide* membership on demand.
2. **Map that intent-data to/from concrete mechanisms with a lens** — a *bidirectional*
   transformation (codegen one way, discovery/reconciliation the other) whose well-behaved
   laws are exactly the correctness guarantee.

Google Zanzibar is the existence proof for (1) at planet scale (relation tuples + rewrite
rules + a snapshot consistency model). Schema-as-data (Rule4) is the existence proof for the
representation; DuckLake (bitemporal time-travel) is the existence proof for the audit; the
`sampling → provision → reconcile` pattern (already built in `column_role/`) is the
existence proof for the lifecycle. **This is not a new ETOOBIG problem — it's the schema
work pointed at a different cartesian product.**

## The dimensions are each schema-as-data

"A database is a thing with attributes" generalises: a **principal** is a thing with
attributes, a **resource** is a thing with attributes, a **service/verb** is a thing with
attributes. So `{P}`, `{R}`, `{S}` are each a `column_role`-shaped catalog — append-only,
freely denormalised, bitemporal, with a *grouping discriminator* exactly like
column_role's `table | index | pk | fk`:

```
principal(dataserver, database, sample_time,          -- the (dataserver, database, sample_time) widening, as in column_role
          principal_kind,            -- user | group | service_account | machine | role
          name, realm,               -- paulharrington@PHRRNGTN.ARPA
          member_of,                 -- group closure (intensional: a row per edge)
          attrs)                     -- free JSON: dept, scope, …

resource(dataserver, database, sample_time,
         resource_kind,             -- server | database | schema | table | column | file | topic | host | network
         name, parent,              -- hierarchy: dbo.cust ∈ rule4_test ∈ gfe
         attrs)                     -- classification, owner, sensitivity, …

service(dataserver, database, sample_time,
        service_kind,              -- verb | capability | scope
        name,                      -- connect | select | insert | ddl | reach | impersonate
        applies_to_kind,           -- which resource_kind it's meaningful on
        attrs)
```

Each is written through `ducklake_oob_writer` the same way `column_role` is, so
`schema_as_of(T)` already works on them.

## The product is a `grants` relation of *rules*, not tuples

You do **not** store `(p, r, s)` cells. You store selectors:

```
grant(dataserver, database, sample_time,
      principal_selector,   -- e.g. group='dba'  OR  name='paulharrington'  OR  attr.dept='data'
      service_selector,     -- e.g. name IN ('connect','select')  OR  kind='verb'
      resource_selector,    -- e.g. parent='rule4_test'  OR  name LIKE 'dbo.%'  OR  attr.sensitivity<'high'
      effect)               -- allow | deny  (deny wins, evaluated last)
```

- **RBAC** = a `grant` whose principal_selector is a group/role and service/resource
  selectors are sets.
- **ABAC** = selectors that are predicates over `attrs`.
- **ReBAC / Zanzibar** = selectors that traverse `member_of` / `parent` relations (bounded
  rewrite).

Pick the weakest language that covers the case; add predicates only where the structure
can't.

## `access_as_of(T)`: decision and audit, bitemporal

"Does P have S on R as of T?" is: resolve the catalogs **as of T**, expand the selectors
(member_of / parent closure, depth-bounded), and check for an `allow` not overridden by a
`deny`:

```sql
-- decision (bind :p, :s, :r, :t); audit = drop the binds and SELECT the matrix
WITH P AS (SELECT * FROM principal WHERE sample_time = (SELECT max(sample_time) FROM principal WHERE sample_time <= :t)),
     R AS (SELECT * FROM resource  WHERE sample_time = (SELECT max(sample_time) FROM resource  WHERE sample_time <= :t)),
     G AS (SELECT * FROM grant     WHERE sample_time = (SELECT max(sample_time) FROM grant     WHERE sample_time <= :t)),
     -- transitive closures (bounded): principal ∈ group*, resource ∈ parent*
     P_CLOSURE AS (... recursive over member_of ...),
     R_CLOSURE AS (... recursive over parent ...)
SELECT bool_or(g.effect = 'allow') AND NOT bool_or(g.effect = 'deny') AS allowed
FROM G AS g
WHERE matches(g.principal_selector, :p, P_CLOSURE)
  AND matches(g.service_selector,   :s)
  AND matches(g.resource_selector,  :r, R_CLOSURE)
```

The product is never materialised; you evaluate the *finite* rule set against the *small*
closures. Auditability — "who had what, as of when" — is the same query without the binds.

## The lens to concrete mechanisms (the codegen, and why it's correct)

The intent-data is the **source of truth**. Each mechanism (AD, LDAP, SQL Server logins/
GRANTs, PostgreSQL roles/`pg_hba`/GRANTs, Kerberos SPNs/keytabs, Tailscale ACLs) is reached
through an **asymmetric lens** `(get, put)`:

- **`get : mechanism → intent`** — *discover/import* the live state and project it into the
  catalogs/grants. (Read AD users+groups, SQL `sys.database_principals` + `sys.database_permissions`,
  PG `pg_roles` + `information_schema.role_table_grants`, registered SPNs, …)
- **`put : (intent, mechanism) → mechanism`** — *generate/apply* the concrete config from
  the intent against the current state: `CREATE LOGIN … FROM WINDOWS`, `GRANT SELECT …`,
  `samba-tool group addmembers`, a `pg_hba` line, `samba-tool spn add` + keytab export, a
  Tailscale ACL entry.

It's **asymmetric** because the mechanism holds more state than the intent manages — the
lens focuses on a *subset* and must preserve the rest (the *complement*). The
**well-behavedness laws are the correctness contract**:

- **GetPut** `put(s, get(s)) = s` — applying what you just read changes nothing → a clean
  reconcile is a no-op (idempotent; no churn).
- **PutGet** `get(put(s, a)) = a` — after applying intent `a`, reading back yields `a` → the
  apply is faithful (no silent drift introduced by codegen).

Violations of these laws are *exactly* the bugs you fear; making the lens well-behaved is
how you get correctness instead of hoping for it. **Drift** is where `get(mechanism) ≠ intent`
— surfaced as a diff, the same way `reconcile_columns` surfaces missing columns.

## Integrate, don't reinvent: the intent is a *control plane* over battle-tested enforcers

The intent-data layer is a **control plane** — the declarative source of truth. It does
**not** enforce, and it need not *decide* at runtime. Both are delegated to proven systems,
most already in this stack. Keep the classic split:

- **PEP — enforcement** stays native. SQL Server (logins / database roles / `GRANT`),
  PostgreSQL (roles / `GRANT` / RLS / `pg_hba`), Samba **AD** (group membership → access),
  **Kerberos** (authn), **Tailscale** (network ACLs), Forgejo (mTLS). You never write a
  runtime permission check — the engine that owns the resource enforces it, because that's
  the thing that's actually battle-tested.
- **PDP — decision**, where you need policy *beyond* a single native system (cross-system
  ABAC / ReBAC), is an **integration, not a build**: **Keycloak** (already here — OIDC IdP,
  realm roles, token mappers), **OPA/Rego**, or a Zanzibar implementation (**SpiceDB** /
  **OpenFGA**). Don't write your own Zanzibar; `get`/`put` against one.

So the `access_as_of` evaluation earlier is the *fallback*, for the thin cross-cutting cases
no single enforcer covers — not the primary path. The primary path is: the intent is
**compiled down** (`put`) into each enforcer's own native config, and each enforcer decides
for its own resources. The lens keeps your spec as data **and** keeps the enforcers
authoritative: `put` configures them, `get` imports their truth, the laws keep the two
honest.

What the stack already provides (so the work is the *data + lens*, not the enforcers):

| Concern | Battle-tested mechanism (in stack) | Lens target |
|---|---|---|
| Authentication | Kerberos / Samba AD KDC | tickets (pre-flight); no `put` needed |
| Principals & groups | Samba AD (LDAP) | `samba-tool` / `ldbmodify` |
| Federated identity, roles, OIDC | **Keycloak** (realm `lake`) | Keycloak admin API (clients/roles/mappers) |
| Network reachability | Tailscale | tailnet ACL policy |
| SQL data access | SQL Server native authz | logins / roles / `GRANT` (T-SQL) |
| Relational data access | PostgreSQL native authz | roles / `GRANT` / RLS / `pg_hba` |
| Service identity (mTLS) | cert authority (Forgejo pattern) | cert issuance |

The novel part you actually own is small: the **three catalogs + the `grant` rules** (the
intent), and the **lens adapters** (thin `get`/`put` per enforcer). Everything that's hard
to get right *and* hard to build — crypto, replication, consistency, the actual access
checks — you inherit.

## The lifecycle is `sampling → provision → reconcile`, generalised

This is the pattern already built in `column_role/sampling.py`, pointed at entitlements:

| schema work (built) | asset management (this doc) |
|---|---|
| `column_role` capture | `principal` / `resource` / `service` catalogs (`get` from the mechanisms) |
| `SamplingPlan` (desired tables) | the `grant` relation (desired access — the intent) |
| `provision` (create/evolve replica) | `put` — generate the AD/SQL/PG config from the intent |
| `reconcile_columns` (diff → add) | reconcile — `get` actual, diff vs intent, drift report |
| `schema_as_of(T)` | `access_as_of(T)` — audit |

## The concrete corner (start here, not at ETOOBIG)

A real, small instance you already have:

- **P** = `paulharrington` (user), `sql_gfe`, `pg_gfe` (service accounts), `dba` (group,
  `paulharrington ∈ dba`).
- **S** = `connect`, `select`, `ddl` (SQL Server); `connect`, `select` (PG); `reach` (mesh);
  `register_spn` (AD).
- **R** = `gfe` (server/host), `rule4_test` (database), `dbo.cust` (table), the `column_role`
  registry, the tailnet (network).
- **grants** (rules): `dba → {connect,select,ddl} on parent='rule4_test'`;
  `sql_gfe → register_spn on MSSQLSvc/gfe*`; `pg_gfe → register_spn on postgres/gfe`;
  everyone-in-realm `→ reach on tailnet`.

Three catalogs + one `grants` table in DuckLake; three lens adapters (AD via
`samba-tool`/`ldbmodify`, SQL Server via T-SQL, PG via SQL). It's small, correct, real — and
the *same machinery* scales without materialising the product.

## Correctness, summarised

1. **Decidable membership** — bounded closure/rewrite; always terminates with a correct
   yes/no.
2. **Auditable** — bitemporal `access_as_of(T)`; who had what, when (DuckLake time-travel).
3. **No illegal states** — set-based DML; the database enforces, the app doesn't reason.
4. **Lens laws** — GetPut/PutGet give faithful codegen + drift-free reconciliation.
5. **Testability** — test the *rules* (finite) and *sample* the product (the full product is
   untestable; a sample is sound).

## Where it meets the transport

The mesh + Kerberos substrate proves **who** (authn); this layer decides **what they may do**
(authz); the lens **manifests both** onto the concrete mechanisms. The application still just
"talks to names as me" — naming, routing, TLS/mTLS, and pre-flight auth pushed down to the
transport (see *thin-app-over-smart-transport*), and now *authorization itself* reduced to
data with a correct mapping to the mechanisms that enforce it.
