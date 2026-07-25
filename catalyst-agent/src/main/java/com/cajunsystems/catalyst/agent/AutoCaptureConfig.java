package com.cajunsystems.catalyst.agent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * What the agent instruments, and which nondeterminism it captures.
 *
 * <p>Packages are named explicitly and there is no default. Auto-capture rewrites call sites, so the
 * blast radius has to be something the author chose: instrumenting a whole classpath would rewrite
 * library internals whose {@code nanoTime} calls are none of Catalyst's business, and would grow the
 * event log without making anything more replayable.
 *
 * <p>From the command line:
 * <pre>{@code
 * -javaagent:catalyst-agent.jar=packages=com.acme.tasks,com.acme.flows;sources=time,identity
 * }</pre>
 * or programmatically via {@link AutoCaptureConfig#forPackages} and {@link AutoCaptureAgent#install}.
 */
public final class AutoCaptureConfig {

    /**
     * Prefixes that are never instrumented, whatever the configuration says.
     *
     * <p>{@code capture} is the hard one: the bridge implements each capture by performing the very
     * JDK call it replaces, so rewriting it would turn every capture into unbounded recursion. The
     * remaining Catalyst packages are the engine itself — its clock reads are how events get their
     * timestamps, not task behaviour to record. The JDK and Byte Buddy are excluded because
     * instrumenting the instrumentation (or the classes it is built from) deadlocks classloading.
     */
    private static final List<String> NEVER_INSTRUMENT = List.of(
            "com.cajunsystems.catalyst.capture.",
            "com.cajunsystems.catalyst.engine.",
            "com.cajunsystems.catalyst.runtime.",
            "com.cajunsystems.catalyst.events.",
            "com.cajunsystems.catalyst.log.",
            "net.bytebuddy.",
            "java.", "javax.", "jdk.", "sun.", "com.sun.");

    private final Set<String> packages;
    private final Set<CaptureSource> sources;

    private AutoCaptureConfig(Set<String> packages, Set<CaptureSource> sources) {
        if (packages.isEmpty()) {
            throw new IllegalArgumentException("Auto-capture needs at least one package to instrument, "
                    + "e.g. packages=com.acme.tasks. Catalyst does not instrument the whole classpath by "
                    + "default: rewriting call sites is opt-in per package.");
        }
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("Auto-capture needs at least one capture source "
                    + "(time, identity, random) — or 'all'.");
        }
        for (String p : packages) {
            for (String denied : NEVER_INSTRUMENT) {
                if (p.startsWith(denied) || denied.startsWith(p)) {
                    throw new IllegalArgumentException("Refusing to instrument '" + p
                            + "': it overlaps '" + denied + "', which auto-capture must never rewrite.");
                }
            }
        }
        this.packages = Set.copyOf(packages);
        this.sources = EnumSet.copyOf(sources);
    }

    /** Instruments the given packages (and their subpackages), capturing every source. */
    public static AutoCaptureConfig forPackages(String... packages) {
        return new AutoCaptureConfig(new LinkedHashSet<>(Arrays.asList(packages)),
                EnumSet.allOf(CaptureSource.class));
    }

    /** Instruments the given packages, capturing only the named sources. */
    public static AutoCaptureConfig of(Collection<String> packages, Collection<CaptureSource> sources) {
        return new AutoCaptureConfig(new LinkedHashSet<>(packages), EnumSet.copyOf(sources));
    }

    /** A copy of this config restricted to {@code sources}. */
    public AutoCaptureConfig withSources(CaptureSource... sources) {
        return new AutoCaptureConfig(packages, EnumSet.copyOf(Arrays.asList(sources)));
    }

    /**
     * Parses an agent argument string: {@code packages=a.b,c.d;sources=time,random}. {@code sources} is
     * optional and defaults to all. Unknown keys are rejected rather than ignored — a typo in a
     * {@code -javaagent} argument would otherwise silently capture nothing.
     */
    public static AutoCaptureConfig parse(String agentArgs) {
        if (agentArgs == null || agentArgs.isBlank()) {
            throw new IllegalArgumentException("The auto-capture agent needs arguments, e.g. "
                    + "-javaagent:catalyst-agent.jar=packages=com.acme.tasks");
        }
        List<String> packages = new ArrayList<>();
        Set<CaptureSource> sources = EnumSet.allOf(CaptureSource.class);
        for (String pair : agentArgs.split(";")) {
            if (pair.isBlank()) continue;
            int eq = pair.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("Malformed agent argument '" + pair
                        + "'. Expected key=value pairs separated by ';'.");
            }
            String key = pair.substring(0, eq).trim().toLowerCase(Locale.ROOT);
            String value = pair.substring(eq + 1).trim();
            switch (key) {
                case "packages" -> Arrays.stream(value.split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).forEach(packages::add);
                case "sources" -> sources = CaptureSource.parse(value);
                default -> throw new IllegalArgumentException("Unknown agent argument '" + key
                        + "'. Known arguments: packages, sources.");
            }
        }
        return new AutoCaptureConfig(new LinkedHashSet<>(packages), sources);
    }

    /** True if a class with this binary name sits in an instrumented package. */
    public boolean instruments(String className) {
        if (className == null) return false;
        for (String denied : NEVER_INSTRUMENT) {
            if (className.startsWith(denied)) return false;
        }
        for (String p : packages) {
            if (className.startsWith(p + ".") || className.equals(p)) return true;
        }
        return false;
    }

    public Set<String> packages() {
        return packages;
    }

    public Set<CaptureSource> sources() {
        return sources;
    }

    @Override
    public String toString() {
        return "packages=" + String.join(",", packages) + "; sources="
                + sources.stream().map(s -> s.name().toLowerCase(Locale.ROOT)).sorted().toList();
    }
}
