package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.events.EventJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadCodecTest {

    record Point(int x, int y) {}

    enum Color { RED, GREEN, BLUE }

    private final PayloadCodec codec = new PayloadCodec();

    @SuppressWarnings("unchecked")
    private <T> T roundTrip(T value) {
        return (T) codec.fromTree(codec.toTree(value));
    }

    @Test
    void roundTripsRecordsWithConcreteType() {
        Point p = new Point(3, 4);
        JsonNode node = codec.toTree(p);
        Object back = codec.fromTree(node);
        assertThat(back).isInstanceOf(Point.class).isEqualTo(p);
    }

    @Test
    void roundTripsPrimitivesAndStrings() {
        assertThat(codec.fromTree(codec.toTree("hello"))).isEqualTo("hello");
        assertThat(codec.fromTree(codec.toTree(42))).isEqualTo(42);
        assertThat(codec.fromTree(codec.toTree(true))).isEqualTo(true);
    }

    @Test
    void roundTripsListsWithElementTypeFidelity() {
        List<Point> points = List.of(new Point(1, 2), new Point(3, 4));
        Object back = roundTrip(points);
        assertThat(back).isInstanceOf(List.class).isEqualTo(points);
        assertThat(((List<?>) back).get(0)).isInstanceOf(Point.class); // not a LinkedHashMap
    }

    @Test
    void roundTripsSetsAndEnums() {
        Set<Color> colors = Set.of(Color.RED, Color.BLUE);
        Object back = roundTrip(colors);
        assertThat(back).isInstanceOf(Set.class).isEqualTo(colors);
        assertThat(((Set<?>) back).iterator().next()).isInstanceOf(Color.class);
    }

    @Test
    void roundTripsMapsWithNonStringKeysAndTypedValues() {
        Map<Point, Color> map = Map.of(new Point(0, 0), Color.RED, new Point(9, 9), Color.GREEN);
        Object back = roundTrip(map);
        assertThat(back).isInstanceOf(Map.class).isEqualTo(map);
        Map.Entry<?, ?> entry = ((Map<?, ?>) back).entrySet().iterator().next();
        assertThat(entry.getKey()).isInstanceOf(Point.class);
        assertThat(entry.getValue()).isInstanceOf(Color.class);
    }

    @Test
    void roundTripsNestedCollections() {
        Map<String, List<Point>> nested = Map.of(
                "a", List.of(new Point(1, 1)),
                "b", List.of(new Point(2, 2), new Point(3, 3)));
        assertThat(roundTrip(nested)).isEqualTo(nested);
    }

    @Test
    void roundTripsPrimitiveAndObjectArrays() {
        int[] ints = {1, 2, 3};
        assertThat((int[]) roundTrip(ints)).containsExactly(1, 2, 3);

        Point[] points = {new Point(1, 2), new Point(3, 4)};
        Point[] back = roundTrip(points);
        assertThat(back).containsExactly(points);

        int[][] nested = {{1, 2}, {3}};
        int[][] backNested = roundTrip(nested);
        assertThat(backNested[0]).containsExactly(1, 2);
        assertThat(backNested[1]).containsExactly(3);
    }

    @Test
    void roundTripsHeterogeneousObjectAndAbstractComponentArrays() {
        Object[] mixed = {"x", new Point(1, 2), Color.RED};
        Object[] backMixed = roundTrip(mixed);
        assertThat(backMixed).containsExactly("x", new Point(1, 2), Color.RED);
        assertThat(backMixed.getClass().getComponentType()).isEqualTo(Object.class);

        Number[] numbers = {1, 2L, 3.0};
        Number[] backNumbers = roundTrip(numbers);
        assertThat(backNumbers).containsExactly(1, 2L, 3.0);
        assertThat(backNumbers.getClass().getComponentType()).isEqualTo(Number.class);
    }

    @Test
    void handlesNullsInsideCollections() {
        java.util.List<String> withNull = new java.util.ArrayList<>();
        withNull.add("x");
        withNull.add(null);
        assertThat(roundTrip(withNull)).isEqualTo(withNull);
    }

    @Test
    void refusesNonAllowlistedTypeFromTamperedLog() {
        // A crafted envelope naming an arbitrary class must be rejected, not loaded and deserialized.
        ObjectNode tampered = EventJson.newMapper().createObjectNode();
        tampered.put("type", "java.io.File");
        tampered.set("value", new TextNode("/etc/passwd"));

        assertThatThrownBy(() -> codec.fromTree(tampered))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("non-allowlisted");
    }

    @Test
    void refusesNonAllowlistedTypeNestedInsideACollection() {
        // The allowlist must hold at every leaf, however deeply nested — a list whose element envelope
        // names an arbitrary class must be rejected.
        var m = EventJson.newMapper();
        ObjectNode evilLeaf = m.createObjectNode();
        evilLeaf.put("type", "java.io.File");
        evilLeaf.set("value", new TextNode("/etc/passwd"));
        ArrayNode elements = m.createArrayNode();
        elements.add(evilLeaf);
        ObjectNode listEnvelope = m.createObjectNode();
        listEnvelope.put("type", "[]list");
        listEnvelope.set("value", elements);

        assertThatThrownBy(() -> codec.fromTree(listEnvelope))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("non-allowlisted");
    }

    /**
     * Auto-capture records these as effect values, so each must survive the envelope round-trip
     * exactly — a value that came back merely equivalent (a normalised zone, a truncated fraction)
     * would replay as a different value than the one the task originally saw.
     */
    @Test
    void roundTripsTheValueTypesAutoCaptureRecords() {
        assertThat(roundTrip(UUID.fromString("3f2504e0-4f89-11d3-9a0c-0305e82c3301")))
                .isEqualTo(UUID.fromString("3f2504e0-4f89-11d3-9a0c-0305e82c3301"));
        assertThat(roundTrip(Instant.parse("2024-05-06T07:08:09.123456789Z")))
                .isEqualTo(Instant.parse("2024-05-06T07:08:09.123456789Z"));
        assertThat(roundTrip(LocalDate.of(2024, 5, 6))).isEqualTo(LocalDate.of(2024, 5, 6));
        assertThat(roundTrip(LocalTime.of(7, 8, 9, 123456789))).isEqualTo(LocalTime.of(7, 8, 9, 123456789));
        assertThat(roundTrip(LocalDateTime.of(2024, 5, 6, 7, 8, 9, 123456789)))
                .isEqualTo(LocalDateTime.of(2024, 5, 6, 7, 8, 9, 123456789));
        assertThat(roundTrip(OffsetDateTime.parse("2024-05-06T07:08:09.123456789+02:00")))
                .isEqualTo(OffsetDateTime.parse("2024-05-06T07:08:09.123456789+02:00"));
        assertThat(roundTrip(ZonedDateTime.parse("2024-05-06T07:08:09.123456789+02:00[Europe/Paris]")))
                .isEqualTo(ZonedDateTime.parse("2024-05-06T07:08:09.123456789+02:00[Europe/Paris]"));
    }

    /** Live clock values, not just literals — whatever sub-second precision the platform yields survives. */
    @Test
    void roundTripsLiveClockValuesWithFullPrecision() {
        Instant instant = Instant.now();
        assertThat(roundTrip(instant)).isEqualTo(instant);

        LocalDateTime localDateTime = LocalDateTime.now();
        assertThat(roundTrip(localDateTime)).isEqualTo(localDateTime);

        ZonedDateTime zoned = ZonedDateTime.now();
        assertThat(roundTrip(zoned)).isEqualTo(zoned);

        UUID uuid = UUID.randomUUID();
        assertThat(roundTrip(uuid)).isEqualTo(uuid);
    }

    @Test
    void refusesNonAllowlistedArrayComponentType() {
        var m = EventJson.newMapper();
        ObjectNode arrayEnvelope = m.createObjectNode();
        arrayEnvelope.put("type", "[]array:java.io.File");
        arrayEnvelope.set("value", m.createArrayNode());

        assertThatThrownBy(() -> codec.fromTree(arrayEnvelope))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("non-allowlisted");
    }
}
