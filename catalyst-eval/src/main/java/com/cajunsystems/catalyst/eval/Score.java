package com.cajunsystems.catalyst.eval;

/**
 * One scorer's verdict on one case.
 *
 * @param scorer the scorer's name
 * @param value  a number the scorer reports (1/0 for a check, a ratio, a delta...); averaged in the report
 * @param pass   whether the case meets the scorer's bar; the report gives the pass rate
 * @param detail a short human explanation, shown for failing cases
 */
public record Score(String scorer, double value, boolean pass, String detail) {

    public static Score pass(String scorer, double value, String detail) {
        return new Score(scorer, value, true, detail);
    }

    public static Score fail(String scorer, double value, String detail) {
        return new Score(scorer, value, false, detail);
    }
}
