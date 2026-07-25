package com.cajunsystems.catalyst.agent;

import com.cajunsystems.catalyst.capture.AutoCapture;
import net.bytebuddy.asm.MemberSubstitution;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.random.RandomGenerator;

/**
 * Builds the call-site rewriting rules: for each JDK nondeterminism source, an invocation inside an
 * instrumented class is redirected to the matching {@link AutoCapture} bridge method.
 *
 * <p>Only the invocation is rewritten — the surrounding method keeps its shape, which is why the agent
 * can also retransform classes that are already loaded. An instance draw passes its receiver as the
 * bridge method's first parameter; that is Byte Buddy's calling convention for substituting a virtual
 * call, and it is what lets the bridge record the value <em>that</em> generator produced.
 */
final class Substitutions {

    private Substitutions() {}

    static MemberSubstitution build(AutoCaptureConfig config) {
        MemberSubstitution substitution = MemberSubstitution.relaxed();

        if (config.sources().contains(CaptureSource.TIME)) {
            // Only the no-argument factories. An overload taking a Clock or ZoneId is the caller
            // already choosing its own time source, and Catalyst does not override that choice.
            substitution = replace(substitution, method(Instant.class, "now"), bridge("now"));
            substitution = replace(substitution, method(System.class, "currentTimeMillis"),
                    bridge("currentTimeMillis"));
            substitution = replace(substitution, method(System.class, "nanoTime"), bridge("nanoTime"));
            substitution = replace(substitution, method(LocalDate.class, "now"), bridge("localDateNow"));
            substitution = replace(substitution, method(LocalTime.class, "now"), bridge("localTimeNow"));
            substitution = replace(substitution, method(LocalDateTime.class, "now"),
                    bridge("localDateTimeNow"));
            substitution = replace(substitution, method(ZonedDateTime.class, "now"),
                    bridge("zonedDateTimeNow"));
            substitution = replace(substitution, method(OffsetDateTime.class, "now"),
                    bridge("offsetDateTimeNow"));
        }

        if (config.sources().contains(CaptureSource.IDENTITY)) {
            substitution = replace(substitution, method(UUID.class, "randomUUID"), bridge("randomUUID"));
        }

        if (config.sources().contains(CaptureSource.RANDOM)) {
            substitution = replace(substitution, method(Math.class, "random"), bridge("mathRandom"));
            // Matched by declaring type rather than by an exact Method so every generator is covered:
            // Random and ThreadLocalRandom declare their own overrides, while nextInt(int,int) is only
            // declared on the RandomGenerator interface.
            substitution = replace(substitution, draw("nextInt"),
                    bridge("nextInt", RandomGenerator.class));
            substitution = replace(substitution, draw("nextInt", int.class),
                    bridge("nextInt", RandomGenerator.class, int.class));
            substitution = replace(substitution, draw("nextInt", int.class, int.class),
                    bridge("nextInt", RandomGenerator.class, int.class, int.class));
            substitution = replace(substitution, draw("nextLong"),
                    bridge("nextLong", RandomGenerator.class));
            substitution = replace(substitution, draw("nextDouble"),
                    bridge("nextDouble", RandomGenerator.class));
            substitution = replace(substitution, draw("nextFloat"),
                    bridge("nextFloat", RandomGenerator.class));
            substitution = replace(substitution, draw("nextBoolean"),
                    bridge("nextBoolean", RandomGenerator.class));
            substitution = replace(substitution, draw("nextGaussian"),
                    bridge("nextGaussian", RandomGenerator.class));
            substitution = replace(substitution, draw("nextBytes", byte[].class),
                    bridge("nextBytes", RandomGenerator.class, byte[].class));
        }

        return substitution;
    }

    private static MemberSubstitution replace(MemberSubstitution substitution,
                                              ElementMatcher.Junction<MethodDescription> matcher,
                                              Method replacement) {
        return substitution.method(matcher).replaceWith(replacement);
    }

    /** Matches exactly one JDK method. */
    private static ElementMatcher.Junction<MethodDescription> method(Class<?> owner, String name,
                                                                    Class<?>... parameters) {
        return ElementMatchers.is(jdkMethod(owner, name, parameters));
    }

    /** Matches a draw of the given shape on any {@link RandomGenerator} implementation. */
    private static ElementMatcher.Junction<MethodDescription> draw(String name, Class<?>... parameters) {
        return ElementMatchers.<MethodDescription>isDeclaredBy(
                        ElementMatchers.isSubTypeOf(RandomGenerator.class))
                .and(ElementMatchers.named(name))
                .and(ElementMatchers.takesArguments(parameters));
    }

    private static Method jdkMethod(Class<?> owner, String name, Class<?>... parameters) {
        try {
            return owner.getMethod(name, parameters);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Missing JDK method " + owner.getName() + "." + name, e);
        }
    }

    private static Method bridge(String name, Class<?>... parameters) {
        try {
            return AutoCapture.class.getMethod(name, parameters);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Missing AutoCapture bridge method " + name, e);
        }
    }
}
