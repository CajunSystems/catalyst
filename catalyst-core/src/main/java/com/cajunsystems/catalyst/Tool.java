package com.cajunsystems.catalyst;

/**
 * A tool: a typed function invoked from a task via {@link Context#call}. Input and output are
 * records so their schemas can be derived by reflection. Tools are invoked only through
 * {@code ctx.call(tool, input)} so that every invocation is recorded and substitutable.
 *
 * <p>Pure tools may be annotated {@link Deterministic}: on replay Catalyst re-executes them instead
 * of storing their (possibly large) output.
 *
 * @param <I> input type (a record)
 * @param <O> output type
 */
public interface Tool<I, O> {

    /** Stable identifier used in the timeline and to correlate recorded invocations. */
    String name();

    /** The input type, used to derive a schema and to deserialize recorded inputs. */
    Class<I> inputType();

    /** Applies the tool to its input. */
    O apply(I input) throws Exception;
}
