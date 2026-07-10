package com.cajunsystems.catalyst;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@link Tool} as a pure function of its input. On replay, Catalyst re-executes a
 * deterministic tool rather than substituting its recorded output — cheaper for large outputs and
 * safe because the result is guaranteed to match (spec §4).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Deterministic {
}
