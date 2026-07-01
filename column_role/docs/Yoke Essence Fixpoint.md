# Yoke, Essence, Fixpoint

*Three words that name why the catalog-as-data move works, and how far the guarantee reaches. A conceptual companion to [[Catalog As Data]] (cited below as CAD).*

This note fixes vocabulary. It names the pre-essence referent (**yoke**), the projected definition (**essence**), and the structural property that makes the reflexive move sound (**fixpoint**) — and, as importantly, marks where each claim stops. Everything here is downstream of Codd's Rule 4 and the [[Column Role Metamodel]].

> **Attribution.** The construction and its working vocabulary — Rule4, the *yoke*, "can't-not-work," SELECT-as-transposition — are Paul's. The correspondence to formal terms — fixed point, least fixed point, idempotence, the Lockean nominal/real-essence distinction as applied here, the `*yug-*` etymology — was worked out with Claude in conversation. That correspondence is an *analogy under test*, not a proof; §6 corrects one place where the analogy first over-reached. The point of writing it down is to make the intuition communicable, not to certify it.

---

## 1. The yoke — a name for the pre-essence referent

The meta-schema's job (CAD §4–6) is to project an **essence**, a chosen bundle of attributes, from an authoritative catalog relation. That raises a prior question: what do you call the thing *before* projection — the opaque referent on the dataserver you point the machine at but have not yet decided how to define?

A **yoke** — Hiberno-English for an under-specified thing, a whatchamacallit — is the name for that state. "Hand me that yoke" asserts the thing is real, present, and pointable-at, and asserts nothing else. It is pure referent with the description deliberately, temporarily withheld. That is what `capture_essence` consumes, so the pipeline reads in one line: **a machine for turning yokes into essences.** Point it at an opaque yoke, get back a named bundle of attributes.

## 2. Why "yoke" and not "object", "entity", or "thing"

The word fills a real lexical gap. Each technical alternative pre-commits to something the architecture defers:

- **object** imports OO baggage — identity, attributes, methods — the commitments we withhold until projection.
- **entity** is worse: it collides with ER-modelling's precisely-named, attributed entities, the opposite of an un-projected referent.
- **thing** carries the Aristotelian *thing-with-an-essence* reading the project rejects (CAD §5: not "a database is a thing with attributes").
- **referent** is exact but cold, with no sense of *deferred* naming.

"Yoke" avoids all four, and has four properties that matter. It is **ontologically empty**, so it holds an un-modelled referent in play without inviting elaboration — a discipline against premature specification. It **carries the invariant in ordinary usage**: calling a thing a yoke makes no claim that could be false, so naming it later can only add structure, never contradict. It is **grain-agnostic** — login, database, schema, column are all just yokes before projection, matching the uniformity the proxy principle claims (CAD §5). And it **names the zero on the resolution dial** (CAD §6): the yoke is resolution zero, no relations closed over. The folk word already is the name for that limit.

## 3. Etymological note

The "thing" sense of *yoke* is the ox-yoke generalised — a piece of apparatus. Old English `geoc`, from Proto-Germanic `*juką`, from PIE `*yugóm`, the "joining" root that also gives Latin `iungere` and English *join* / *junction*.

The operation that resolves a yoke into an essence is, mechanically, the JOIN (CAD §4). So the placeholder-word and the resolution-operator share a root under `*yug-`, "to join." A real shared root, worth one sentence and no more. (Contrast the genuine non-coincidence in §9.)

## 4. Essence — nominal, not real

An **essence** is the named bundle of attributes projected onto a yoke: the answer to "how, for now, shall we define this thing?" The word looks misused, because *essence* usually means a thing's intrinsic what-it-is, independent of us, whereas ours is chosen and shifts with purpose (CAD §6).

The apparent misuse dissolves in **Locke's** sense of the word rather than Aristotle's. Locke splits essence in two. A **real essence** is the hidden internal constitution from which observable properties flow; for substances he holds it is inaccessible — we see the effects, never the constitution. A **nominal essence** is the abstract idea, the bundle of properties we file under a general name: "the workmanship of the understanding," made by us, not discovered.

Every essence here is nominal. We never touch the real constitution of the thing on the dataserver; we project the catalog's *account* of it — the proxy row and the columns we choose to close over. The word is exact, and the irony is only apparent. A database has no accessible real essence to be unfaithful to, so the nominal essence is all there is, and it is ours to compose. Because it is chosen, no single one is privileged: a thing has a *family* of nominal essences, thin to thick, indexed by purpose. The resolution dial is the choice of which one to attach.

## 5. One turn of the machine

Concretely, at the server-object grain. The **yoke** is a database — an opaque object with no attributes to project directly. Its **proxy** is the `sys.databases` row (CAD §5). A **thin essence** is `(name, database_id, state_desc)` — enough to index it. A **thick essence** joins more authoritative relations: `sys.database_files` makes it a storage object, `sys.database_principals` plus permissions make it a security object, `extended_properties` make it classified. Same yoke, one proxy, a family of essences. The dial (CAD §6) is simply *which* relations you close over and *how many*. Nothing is modelled; every attribute in every essence is SELECTed from a relation the server already maintains.

## 6. Fixpoint — idempotence, and the other kind

A **fixed point** of a function `f` is a value `x` with `f(x) = x`: the input the operation returns unchanged.

Let `D` = "represent this as relational data." Point `D` at a database and you get the catalog (`sys.columns`, `sys.objects`, …). Apply `D` again and the catalog already *is* relations, so `D(catalog) = catalog`. The catalog is a fixed point of `D`.

Be honest about *which kind* this is. `D` is **idempotent** — its output is already in relational form, so applying it twice does nothing more than applying it once. The fixed points of an idempotent map are exactly its image, so the catalog is a fixed point for the same modest reason *every* relational representation `D` emits is one. The catalog is not singled out by some deep property; it simply lives in the codomain. The content here is idempotence, and idempotence is exactly what halts the regress: you might expect to need a meta-schema to describe the schema, then a meta-meta-schema, forever, but once `D(x) = x` there is no higher level to climb to. Rule 4 is the design mandate that makes `D` idempotent on schema-description — it requires the self-description to live in the same medium as the data. Real, and clarifying, but modest.

There is a second, genuinely deeper kind of fixpoint in the system, and it should not be confused with the first. Recursive queries — `WITH RECURSIVE`, transitive closure, reachability, Datalog, and the lineage walk of CAD §13 — are **least fixed points** of monotone operators, reached by iterating from ∅ to convergence (Kleene iteration; Knaster–Tarski). Here you *grind up to* the fixed point rather than landing on it by idempotence. (Recursion in general is this: a recursive definition's meaning is the least fixed point of a functional; the Y combinator solves `f = F(f)`.)

The two share only the bare schema `f(x) = x`. The reflexive catalog fixpoint is idempotence; the recursive one is the substantive, iterate-to kind. One word, two very different weights — and the note earlier over-reached by letting the second lend prestige to the first. It does not. Keep them separate.

## 7. What the fixpoint buys, and what it does not

The reflexive fixpoint (§6, idempotence) buys one specific, real thing: at the **description layer**, `D` introduces no new *kind* of object, so no differently-shaped, possibly-wrong representation can be interposed between levels. The catalog's self-description is therefore correct-by-construction, and its only way to fall short is to omit an essence you did not record. *This* is the precise, defensible content of "can't-not-work": at the metadata layer the failure axis is **coverage, not correctness**, because there is no modelling step that could record something wrongly.

The slogan does not extend to the whole pipeline, and it is important to say so plainly. The replica does more than self-describe the catalog: it maps types, round-trips values, and reconstructs temporally at scale. Those are ordinary engineering with ordinary failure modes, and they *can* be wrong, not merely incomplete — `money` mapped to `numeric` is not exact, a "funky" value may not round-trip, inline-MVCC snapshot counts strain under high-frequency CDC. CAD §14 lists these as the replica's real limits, and they are correctness-grade limits at the value grain. The fixpoint says nothing about them. (CAD §11 runs one turn end to end — SQL Server → Change Tracking → DuckLake → SQLite — and marks exactly where this value-grain layer begins.)

So scope the claim: coverage-not-correctness is a property of the **catalog-as-data move**, not a global guarantee over the pipeline that carries the data. Stating it this way makes it stronger, because it is now true where asserted and silent where it would be false.

## 8. SELECT as transposition — an analogy, held to its limit

The project's move has a suggestive echo in the act of *explaining the project*. An intuition can sit as a yoke: real, load-bearing, pointable-at, but un-named in the register others accept as explanation. Making it communicable resembles projection more than translation. Translation risks loss across a foreign medium; projection — SELECT — exhibits what is already there and asserts nothing new. The formal names in this note (fixed point, nominal essence) were not invented for the occasion; they pre-existed it, waiting to be closed over. In that sense explaining the work is SELECTing it into a shareable register, and the honest verb is SELECT, not *transpose*.

Hold the analogy to its limit, because it has one the reflexive fixpoint does not. An explanation is **not** correct-by-construction. There is no catalog behind "the available ideas"; the map from intuition to formalism is exactly the kind of projection that can *mislead*, and §6 is a worked example — the fixpoint analogy first over-reached by dressing idempotence as something deeper, and had to be corrected. The failure mode of an explanation is that it can be wrong, not merely incomplete. The analogy earns its place by illuminating, not by immunity. (`sys.locke` and `sys.fixpoint` are figures of speech, not catalog views.)

What survives, and is genuinely useful: "phase 0" is a starting line, not a finish. The mechanism is built and the move is demonstrated end to end (CAD §15). Every data-centric problem downstream is a yoke sitting still on a dataserver, and the remaining work along the axis the fixpoint governs is coverage — how much of what is already, authoritatively there you bother to SELECT.

## 9. Honest non-coincidence

The `*yug-*` link in §3 is a real shared root. The following is not, and is flagged so it is not mistaken for one. The iterate-to fixed point (§6) is reached by taking **joins** — least upper bounds up a lattice. That "join" is lattice-theoretic, not SQL's relational JOIN. "The resolution operator is JOIN" and "the fixpoints are reached by joins" are both true, in two unrelated vocabularies. A genuine pun, a collision — not family.

---

See also: [[Catalog As Data]], [[Column Role Metamodel]], [[DuckLake OOB Writer]], [[Composable Relation Builders]].
