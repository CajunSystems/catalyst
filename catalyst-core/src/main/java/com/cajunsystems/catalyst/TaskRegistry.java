package com.cajunsystems.catalyst;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps a recorded <em>task type</em> to a {@link TaskFactory} that can reconstruct it, so the
 * runtime can drive a standalone {@code resume(id)} without the caller holding the original
 * {@link Task} (spec §7, v0.2 increment ②). The task type is the string recorded in
 * {@code ExecutionCreated.taskType()} — by default {@code task.getClass().getName()}.
 *
 * <p><strong>Use named task classes.</strong> A lambda's synthetic class name (e.g.
 * {@code …$$Lambda/0x…}) is not stable across JVMs, so a lambda recorded in one process cannot be
 * looked up by type in another. Register named {@code Task} implementations (or an explicit logical
 * type string) when you want durable, cross-process resume-by-id.
 *
 * <p>Registration is thread-safe; a later registration for the same type replaces the earlier one.
 */
public final class TaskRegistry {

    private final ConcurrentHashMap<String, TaskFactory> factories = new ConcurrentHashMap<>();

    /** Registers a factory under an explicit task-type string. */
    public TaskRegistry register(String taskType, TaskFactory factory) {
        factories.put(requireType(taskType), java.util.Objects.requireNonNull(factory, "factory"));
        return this;
    }

    /** Registers a factory under {@code type.getName()} — the type a named {@code Task} class records. */
    public TaskRegistry register(Class<? extends Task<?>> type, TaskFactory factory) {
        return register(java.util.Objects.requireNonNull(type, "type").getName(), factory);
    }

    /**
     * Registers a task instance under its own class name. Because a task is re-invoked on resume with
     * its boundaries substituted from the log, sharing one stateless instance is safe. Prefer a named
     * class here: a lambda's class name is not stable across processes (see the class note).
     */
    public TaskRegistry register(Task<?> task) {
        java.util.Objects.requireNonNull(task, "task");
        return register(task.getClass().getName(), () -> task);
    }

    /** The factory registered for {@code taskType}, if any. */
    public Optional<TaskFactory> lookup(String taskType) {
        return Optional.ofNullable(factories.get(taskType));
    }

    /** Whether a factory is registered for {@code taskType}. */
    public boolean isRegistered(String taskType) {
        return factories.containsKey(taskType);
    }

    private static String requireType(String taskType) {
        java.util.Objects.requireNonNull(taskType, "taskType");
        if (taskType.isBlank()) {
            throw new IllegalArgumentException("taskType must not be blank");
        }
        return taskType;
    }
}
