package com.cajunsystems.catalyst.log;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotTest {

    @Test
    void isAValueTypeOverItsBytes() {
        Snapshot a = new Snapshot(7, new byte[]{1, 2, 3});
        Snapshot b = new Snapshot(7, new byte[]{1, 2, 3}); // same content, different array instance

        assertThat(a).isEqualTo(b);
        assertThat(a).hasSameHashCodeAs(b);
        assertThat(new Snapshot(8, new byte[]{1, 2, 3})).isNotEqualTo(a); // seq differs
        assertThat(new Snapshot(7, new byte[]{9})).isNotEqualTo(a);       // bytes differ
    }

    @Test
    void defensivelyCopiesStateSoTheLogOwnsAnImmutableView() {
        byte[] mutable = {1, 2, 3};
        Snapshot s = new Snapshot(1, mutable);
        mutable[0] = 99;                 // mutate the caller's array after construction
        assertThat(s.state()).containsExactly(1, 2, 3);

        s.state()[0] = 99;               // mutate a returned copy
        assertThat(s.state()).containsExactly(1, 2, 3);
    }
}
