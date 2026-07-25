package com.cajunsystems.catalyst.agent;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;

/**
 * The auto-capture agent (spec §6): rewrites the JDK's nondeterministic sources inside configured task
 * packages so they become recorded Catalyst boundaries, and task authors no longer have to route every
 * {@code Instant.now()} or {@code UUID.randomUUID()} through {@link com.cajunsystems.catalyst.Context#effect}.
 *
 * <p>A rewritten call site targets {@link com.cajunsystems.catalyst.capture.AutoCapture}, which records
 * through whichever {@code Context} the runtime has bound to the executing thread. Outside an execution
 * the bridge performs the plain JDK call, so instrumented classes behave normally in unit tests and
 * ordinary application code.
 *
 * <h2>Attaching at launch (preferred)</h2>
 * <pre>{@code
 * java -javaagent:catalyst-agent.jar=packages=com.acme.tasks -cp ... com.acme.Main
 * }</pre>
 * At launch no task class is loaded yet, so every class is instrumented as it is defined.
 *
 * <h2>Attaching from inside the process</h2>
 * <pre>{@code
 * AutoCaptureAgent.install(AutoCaptureConfig.forPackages("com.acme.tasks"));
 * }</pre>
 * This self-attaches and <em>retransforms</em> classes that are already loaded, which is what makes it
 * usable from a test or a {@code main} that has already touched its task classes. Install before
 * executing anything you intend to replay: a class instrumented halfway through a run would start
 * capturing mid-execution and produce a log whose boundary sequence no replay can match.
 *
 * <h2>What the determinism contract still asks of you</h2>
 * Auto-capture removes the <em>bookkeeping</em>, not the contract. Task code must still take the same
 * path on every run: capture makes each nondeterministic value reproducible, but a task whose sequence
 * of calls varies (branching on unrecorded external state, iterating a {@code HashSet}) still diverges,
 * and strict replay will still say so.
 */
public final class AutoCaptureAgent {

    private static final Logger log = LoggerFactory.getLogger(AutoCaptureAgent.class);

    private AutoCaptureAgent() {}

    /** An active installation. Closing it removes the transformer and reverts instrumented classes. */
    public interface Installation extends AutoCloseable {
        @Override
        void close();
    }

    /** {@code -javaagent} entry point. */
    public static void premain(String agentArgs, Instrumentation instrumentation) {
        install(AutoCaptureConfig.parse(agentArgs), instrumentation);
    }

    /** Attach-to-running-JVM entry point. */
    public static void agentmain(String agentArgs, Instrumentation instrumentation) {
        install(AutoCaptureConfig.parse(agentArgs), instrumentation);
    }

    /**
     * Installs auto-capture into the running JVM, self-attaching the instrumentation API. Equivalent to
     * launching with {@code -javaagent}, except that classes already loaded are retransformed.
     */
    public static Installation install(AutoCaptureConfig config) {
        return install(config, ByteBuddyAgent.install());
    }

    /** Installs auto-capture using an {@link Instrumentation} you already hold. */
    public static Installation install(AutoCaptureConfig config, Instrumentation instrumentation) {
        if (config == null) throw new IllegalArgumentException("config");
        if (!instrumentation.isRetransformClassesSupported()) {
            throw new IllegalStateException("This JVM does not support retransformation, which "
                    + "auto-capture needs to instrument classes that are already loaded.");
        }

        var substitution = Substitutions.build(config); // stateless: built once, applied to every class
        var transformer = new AgentBuilder.Default(byteBuddy())
                // Rewriting call sites never changes a class's shape (no new members, no signature
                // changes), so retransformation of already-loaded classes stays legal.
                .disableClassFormatChanges()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .with(new LoggingListener())
                .type(ElementMatchers.<TypeDescription>none()
                        .or(target -> config.instruments(target.getName())))
                .transform((builder, type, classLoader, module, protectionDomain) ->
                        builder.visit(substitution.on(ElementMatchers.any())))
                .installOn(instrumentation);

        log.debug("Catalyst auto-capture installed: {}", config);
        return () -> transformer.reset(instrumentation, AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
    }

    /**
     * Byte Buddy's default configuration skips synthetic methods, which would silently exempt the most
     * ordinary place for task nondeterminism to live: a lambda body compiles to a synthetic method, so
     * {@code list.forEach(x -> ... Instant.now() ...)} would go uncaptured while the identical call one
     * line earlier was rewritten. Only the default finalizer stays ignored.
     */
    private static ByteBuddy byteBuddy() {
        return new ByteBuddy().ignore(ElementMatchers.isDefaultFinalizer());
    }

    /**
     * Instrumentation failures are logged rather than thrown: a class the agent cannot rewrite must not
     * take the application down with it. It will simply keep its uncaptured nondeterminism — which
     * strict replay reports later, loudly, at the boundary that diverges.
     */
    private static final class LoggingListener extends AgentBuilder.Listener.Adapter {
        @Override
        public void onTransformation(TypeDescription type, ClassLoader classLoader, JavaModule module,
                                     boolean loaded, DynamicType dynamicType) {
            log.debug("auto-capture instrumented {}{}", type.getName(), loaded ? " (retransformed)" : "");
        }

        @Override
        public void onError(String typeName, ClassLoader classLoader, JavaModule module,
                            boolean loaded, Throwable throwable) {
            log.warn("auto-capture could not instrument {} — its nondeterminism stays uncaptured",
                    typeName, throwable);
        }
    }
}
