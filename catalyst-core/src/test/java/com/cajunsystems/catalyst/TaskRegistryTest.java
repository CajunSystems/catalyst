package com.cajunsystems.catalyst;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskRegistryTest {

    static final class NamedTask implements Task<String> {
        @Override public String execute(Context ctx) { return "ok"; }
    }

    @Test
    void registersAnInstanceUnderItsClassName() {
        NamedTask task = new NamedTask();
        TaskRegistry registry = new TaskRegistry().register(task);

        assertThat(registry.isRegistered(NamedTask.class.getName())).isTrue();
        assertThat(registry.lookup(NamedTask.class.getName())).isPresent();
        // A task is re-invoked on resume, so the factory hands back the same stateless instance.
        assertThat(registry.lookup(NamedTask.class.getName()).orElseThrow().create()).isSameAs(task);
    }

    @Test
    void registersUnderAnExplicitTypeAndByClass() {
        NamedTask task = new NamedTask();
        TaskRegistry registry = new TaskRegistry()
                .register("logical:name", () -> task)
                .register(NamedTask.class, () -> task);

        assertThat(registry.lookup("logical:name")).isPresent();
        assertThat(registry.lookup(NamedTask.class.getName())).isPresent();
    }

    @Test
    void lastRegistrationForATypeWins() {
        Task<?> first = new NamedTask();
        Task<?> second = new NamedTask();
        TaskRegistry registry = new TaskRegistry()
                .register("t", () -> first)
                .register("t", () -> second);

        assertThat(registry.lookup("t").orElseThrow().create()).isSameAs(second);
    }

    @Test
    void unknownTypeIsAbsent() {
        assertThat(new TaskRegistry().lookup("nope")).isEmpty();
        assertThat(new TaskRegistry().isRegistered("nope")).isFalse();
    }

    @Test
    void rejectsBlankOrNullType() {
        assertThatThrownBy(() -> new TaskRegistry().register("  ", () -> null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TaskRegistry().register((String) null, () -> null))
                .isInstanceOf(NullPointerException.class);
    }
}
