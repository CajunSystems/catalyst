package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.events.EventJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadCodecTest {

    record Point(int x, int y) {}

    private final PayloadCodec codec = new PayloadCodec();

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
    void refusesNonAllowlistedTypeFromTamperedLog() {
        // A crafted envelope naming an arbitrary class must be rejected, not loaded and deserialized.
        ObjectNode tampered = EventJson.newMapper().createObjectNode();
        tampered.put("type", "java.io.File");
        tampered.set("value", new TextNode("/etc/passwd"));

        assertThatThrownBy(() -> codec.fromTree(tampered))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("non-allowlisted");
    }
}
