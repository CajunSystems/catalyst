package com.cajunsystems.catalyst.tools;

import com.cajunsystems.catalyst.Deterministic;
import com.cajunsystems.catalyst.Tool;

/**
 * A pure arithmetic tool. Marked {@link Deterministic}: it is a pure function of its input, so replay
 * re-executes it rather than storing its output (spec §4).
 */
@Deterministic
public final class CalculatorTool implements Tool<CalculatorTool.Expr, CalculatorTool.Value> {

    /** {@code left op right}, where {@code op} is one of {@code + - * /}. */
    public record Expr(double left, String op, double right) {}

    public record Value(double result) {}

    @Override
    public String name() {
        return "calculator";
    }

    @Override
    public Class<Expr> inputType() {
        return Expr.class;
    }

    @Override
    public Value apply(Expr input) {
        double r = switch (input.op()) {
            case "+" -> input.left() + input.right();
            case "-" -> input.left() - input.right();
            case "*" -> input.left() * input.right();
            case "/" -> {
                if (input.right() == 0.0) throw new ArithmeticException("division by zero");
                yield input.left() / input.right();
            }
            default -> throw new IllegalArgumentException("Unsupported operator: " + input.op());
        };
        return new Value(r);
    }
}
