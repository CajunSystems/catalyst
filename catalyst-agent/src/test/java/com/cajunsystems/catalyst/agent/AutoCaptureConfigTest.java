package com.cajunsystems.catalyst.agent;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AutoCaptureConfigTest {

    @Test
    void parsesPackagesAndDefaultsToEverySource() {
        AutoCaptureConfig config = AutoCaptureConfig.parse("packages=com.acme.tasks,com.acme.flows");

        assertThat(config.packages()).containsExactlyInAnyOrder("com.acme.tasks", "com.acme.flows");
        assertThat(config.sources()).containsExactlyInAnyOrder(
                CaptureSource.TIME, CaptureSource.IDENTITY, CaptureSource.RANDOM);
    }

    @Test
    void parsesAnExplicitSourceSelection() {
        AutoCaptureConfig config = AutoCaptureConfig.parse("packages=com.acme;sources=time,identity");

        assertThat(config.sources()).containsExactlyInAnyOrder(CaptureSource.TIME, CaptureSource.IDENTITY);
    }

    @Test
    void matchesClassesInInstrumentedPackagesAndTheirSubpackages() {
        AutoCaptureConfig config = AutoCaptureConfig.forPackages("com.acme.tasks");

        assertThat(config.instruments("com.acme.tasks.Billing")).isTrue();
        assertThat(config.instruments("com.acme.tasks.nested.Deep")).isTrue();
        assertThat(config.instruments("com.acme.tasks.Billing$Step")).isTrue();
        assertThat(config.instruments("com.acme.tasksextra.Other")).isFalse(); // prefix, not substring
        assertThat(config.instruments("com.other.Thing")).isFalse();
        assertThat(config.instruments(null)).isFalse();
    }

    @Test
    void neverInstrumentsTheBridgeTheJdkOrByteBuddy() {
        AutoCaptureConfig config = AutoCaptureConfig.forPackages("com.acme");

        // Rewriting the bridge would make every capture recurse into itself.
        assertThat(config.instruments("com.cajunsystems.catalyst.capture.AutoCapture")).isFalse();
        assertThat(config.instruments("com.cajunsystems.catalyst.engine.ReplayingContext")).isFalse();
        assertThat(config.instruments("java.util.UUID")).isFalse();
        assertThat(config.instruments("net.bytebuddy.agent.builder.AgentBuilder")).isFalse();
    }

    @Test
    void refusesToInstrumentAPackageThatOverlapsTheDenyList() {
        assertThatThrownBy(() -> AutoCaptureConfig.forPackages("com.cajunsystems.catalyst.capture"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never rewrite");

        // Also refused when the configured prefix would *contain* a denied package.
        assertThatThrownBy(() -> AutoCaptureConfig.forPackages("com.cajunsystems"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never rewrite");

        assertThatThrownBy(() -> AutoCaptureConfig.forPackages("java.util"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never rewrite");
    }

    @Test
    void rejectsConfigurationsThatWouldSilentlyCaptureNothing() {
        // No default package set: instrumenting the whole classpath is not a thing you get by accident.
        assertThatThrownBy(() -> AutoCaptureConfig.parse(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs arguments");
        assertThatThrownBy(() -> AutoCaptureConfig.parse("sources=time"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one package");
        // A typo in a -javaagent argument must fail loudly rather than capture nothing.
        assertThatThrownBy(() -> AutoCaptureConfig.parse("package=com.acme"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown agent argument");
        assertThatThrownBy(() -> AutoCaptureConfig.parse("packages=com.acme;sources=clock"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown capture source");
        assertThatThrownBy(() -> AutoCaptureConfig.parse("packages"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Malformed agent argument");
    }

    @Test
    void narrowsSourcesWithoutChangingPackages() {
        AutoCaptureConfig config = AutoCaptureConfig
                .of(List.of("com.acme"), List.of(CaptureSource.values()))
                .withSources(CaptureSource.IDENTITY);

        assertThat(config.packages()).containsExactly("com.acme");
        assertThat(config.sources()).containsExactly(CaptureSource.IDENTITY);
    }
}
