package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.ExecutionId;

import java.util.Objects;

/**
 * A child execution started with {@code ctx.spawn(task)}: running on its own while the parent
 * carries on. Pass it to {@code ctx.await}, {@code ctx.awaitAll} or {@code ctx.awaitAny} for its
 * outcome. A handle is only meaningful inside the execution that spawned it: its {@code ref} is the
 * child's place in that execution's code, which is what replay matches on.
 *
 * @param <R> the child task's result type
 */
public final class ChildHandle<R> {

    private final String ref;
    private final ExecutionId id;

    public ChildHandle(String ref, ExecutionId id) {
        this.ref = Objects.requireNonNull(ref, "ref");
        this.id = Objects.requireNonNull(id, "id");
    }

    /** The child's place in the parent's code: {@code child-1}, {@code child-2}, … in task order. */
    public String ref() {
        return ref;
    }

    /** The child's own execution id: inspect, replay or branch it like any execution. */
    public ExecutionId id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ChildHandle<?> h && h.ref.equals(ref) && h.id.equals(id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ref, id);
    }

    @Override
    public String toString() {
        return "ChildHandle[" + ref + " -> " + id.value() + "]";
    }
}
