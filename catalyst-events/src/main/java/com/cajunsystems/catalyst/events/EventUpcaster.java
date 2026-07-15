package com.cajunsystems.catalyst.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Migrates a recorded event's raw JSON from an older schema to the current one, on read, before it is
 * bound to a {@link CatalystEvent} (spec §13.4). This is the escape hatch for changes the
 * <em>tolerant reader</em> cannot absorb on its own.
 *
 * <p><strong>Catalyst's schema-evolution policy.</strong> Events live forever, so the schema evolves
 * under two complementary rules:
 * <ul>
 *   <li><strong>Additive changes use the tolerant reader.</strong> The shared mapper ignores unknown
 *       properties and defaults missing ones, so <em>adding</em> a field (new writers, old logs) and
 *       <em>reading</em> a field a newer writer added (old readers, new logs) both work with no code.
 *       Adding a whole new event type is likewise additive.</li>
 *   <li><strong>Structural changes use an upcaster.</strong> Renaming or splitting a field, changing a
 *       field's type, or renaming an event {@code @type} is not additive; an ordered upcaster chain
 *       transforms the old shape to the current one on decode.</li>
 * </ul>
 *
 * <p>An explicit schema version is intentionally <em>not</em> stamped on events yet: the whole schema
 * is v1, so an absent version means v1. The first breaking change stamps a version going forward and
 * registers an upcaster keyed on the absent/old version — deferring that byte-level change until it
 * actually buys something.
 *
 * <p>Upcasters run before blob rehydration is complete? No — they see fully-inlined events (blob
 * references are resolved first), so an upcaster only ever manipulates plain JSON. They must be
 * <strong>idempotent/defensive</strong>: an upcaster may be handed an event already in the current
 * shape and must return it unchanged.
 */
@FunctionalInterface
public interface EventUpcaster {

    /** Returns {@code event} migrated toward the current schema, or unchanged if it does not apply. */
    JsonNode upcast(JsonNode event);

    /** Renames field {@code from} to {@code to} on events of type {@code type} (no-op if absent). */
    static EventUpcaster renameField(String type, String from, String to) {
        return event -> {
            if (event instanceof ObjectNode obj && isType(obj, type) && obj.has(from) && !obj.has(to)) {
                obj.set(to, obj.remove(from));
            }
            return event;
        };
    }

    /** Renames the {@code @type} discriminator from {@code oldType} to {@code newType}. */
    static EventUpcaster renameType(String oldType, String newType) {
        return event -> {
            if (event instanceof ObjectNode obj && isType(obj, oldType)) {
                obj.put("@type", newType);
            }
            return event;
        };
    }

    /** Fills in {@code field} with {@code defaultValue} on events of {@code type} that lack it. */
    static EventUpcaster defaultField(String type, String field, JsonNode defaultValue) {
        return event -> {
            if (event instanceof ObjectNode obj && isType(obj, type) && !obj.has(field)) {
                obj.set(field, defaultValue.deepCopy()); // isolate each insertion from the shared node
            }
            return event;
        };
    }

    private static boolean isType(ObjectNode obj, String type) {
        JsonNode t = obj.get("@type");
        return t != null && t.isTextual() && t.textValue().equals(type);
    }
}
