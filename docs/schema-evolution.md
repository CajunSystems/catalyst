# Schema evolution

Catalyst events are the durable record of every execution — they **live forever**, so the event
schema must be able to change without orphaning old logs. This document records the strategy chosen
for the v0.1 spec's open question §13.4 (*tolerant reader vs. upcasters*). The answer is **both**,
each applied to the kind of change it fits.

## The two rules

### 1. Additive changes → the tolerant reader (no code)

The shared Jackson mapper (`EventJson`) is configured to **ignore unknown properties** and **default
missing ones**, so these changes need no migration code:

- **Adding a field to an event** — old logs (missing the field) decode with the field defaulted
  (`null` for references, `0`/`false` for primitives); a newer writer's logs (carrying the field) are
  read by older code, which simply ignores it.
- **Adding a whole new event type** — old readers never encounter it; new readers bind it normally.

Keep new fields optional (nullable, or with a sensible zero value) and this rule covers the common
case for free.

### 2. Structural changes → an upcaster

Anything that isn't additive — **renaming or splitting a field, changing a field's type, or renaming
an event `@type`** — is handled by an ordered chain of [`EventUpcaster`](../catalyst-events/src/main/java/com/cajunsystems/catalyst/events/EventUpcaster.java)s
applied on **decode**, before the JSON is bound to a `CatalystEvent`:

```java
EventCodec codec = EventCodec.builder()
        .upcaster(EventUpcaster.renameType("TaskCompleted", "ExecutionCompleted"))
        .upcaster(EventUpcaster.renameField("ExecutionFailed", "message", "error"))
        .build();
```

Upcasters compose (v1→v2→v3 all run on a single decode), see **fully-inlined** events (blob
references are rehydrated first), and must be **idempotent/defensive** — an upcaster may be handed an
event already in the current shape and must return it unchanged. `renameField`, `renameType`, and
`defaultField` cover the common migrations; anything bespoke is a lambda over the raw `JsonNode`.

Register upcasters on the durable log: `GumboEventLog.at(path, List.of(upcaster1, upcaster2))`, or
build a codec directly with `EventCodec.builder()` and pass it via `GumboEventLog.at(path, codec)`.

## Schema versioning

An explicit schema version is **intentionally not stamped on events yet**. The whole schema is v1, so
an absent version simply *means* v1 and every event stays byte-for-byte what it is today. The **first
breaking change** is what introduces a version: from then on new writers stamp a version field, and an
upcaster keyed on the absent/old version migrates pre-change events forward. Deferring the stamp until
it buys something keeps today's logs unchanged and avoids a needless format churn.

## What CI guarantees

The `schema` exit demo (gated in CI) decodes a hand-authored log recorded under an **older** schema —
a renamed event type, a renamed field, and an unknown field a newer writer added — and folds it to the
correct `ExecutionState` under the current schema. See `Demo schema` and
`SchemaEvolutionAcceptanceTest` / `SchemaEvolutionTest`.
