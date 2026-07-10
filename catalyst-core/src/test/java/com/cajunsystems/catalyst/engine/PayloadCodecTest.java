package com.cajunsystems.catalyst.engine;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
