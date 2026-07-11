package com.cajunsystems.catalyst.engine;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class CostModelTest {

    @Test
    void freeAlwaysZero() {
        assertThat(CostModel.free().usd(1000, 2000)).isZero();
    }

    @Test
    void perMillionTokensPricesInputAndOutput() {
        CostModel model = CostModel.perMillionTokens(3.0, 15.0);
        // 1M prompt @ $3 + 1M completion @ $15 = $18
        assertThat(model.usd(1_000_000, 1_000_000)).isCloseTo(18.0, within(1e-9));
        // 500 prompt + 1500 completion
        assertThat(model.usd(500, 1500)).isCloseTo((500 * 3.0 + 1500 * 15.0) / 1_000_000.0, within(1e-12));
    }
}
