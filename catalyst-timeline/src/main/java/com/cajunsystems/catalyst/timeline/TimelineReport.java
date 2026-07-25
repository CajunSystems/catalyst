package com.cajunsystems.catalyst.timeline;

import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.Timeline;
import com.cajunsystems.catalyst.engine.TimelineStep;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;

/**
 * Renders an execution's folded timeline as a self-contained HTML page (spec §12) — the read-only view
 * over {@code runtime.inspect(id)}.
 *
 * <p>Read-only and post-hoc, the same shape as {@code catalyst-otel}: it consumes an
 * {@link ExecutionState}, which is itself nothing but a fold of the event log, and installs no runtime
 * hook. The log remains the only source of truth; this renders it. Because the input is a fold, the
 * report for a given log is deterministic — the same execution always produces the same page.
 *
 * <p>The output has no external references at all: styles are inline, there are no scripts, images or
 * fonts to fetch. A report can be attached to a ticket, committed as a build artifact, or opened from
 * a file:// URL with nothing else present.
 *
 * <h2>Escaping</h2>
 * Everything interpolated comes from the log — task types, tool names, effect labels, recorded
 * payloads, error strings — so all of it is HTML-escaped. Log content is data, and a report is likely
 * to be opened in a browser by someone other than whoever produced the execution.
 */
public final class TimelineReport {

    /** Recorded payloads can be large; past this many characters a detail is elided in the report. */
    private static final int MAX_DETAIL_CHARS = 2_000;

    private TimelineReport() {}

    /** Renders {@code state} as a complete HTML document. */
    public static String html(ExecutionState state) {
        if (state == null) throw new IllegalArgumentException("state");
        Timeline timeline = state.timelineView();

        StringBuilder out = new StringBuilder(8_192);
        out.append("<!doctype html>\n<html lang=\"en\">\n<head>\n");
        out.append("<meta charset=\"utf-8\">\n");
        out.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
        out.append("<title>Catalyst execution ").append(escape(idOf(state))).append("</title>\n");
        out.append("<style>\n").append(css()).append("</style>\n");
        out.append("</head>\n<body>\n");

        appendHeader(out, state);
        appendSummary(out, state, timeline);
        appendSteps(out, state);
        appendFooter(out);

        out.append("</body>\n</html>\n");
        return out.toString();
    }

    /** Renders {@code state} and writes it to {@code file}, creating parent directories as needed. */
    public static Path writeTo(ExecutionState state, Path file) {
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            Files.writeString(file, html(state), StandardCharsets.UTF_8);
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write timeline report to " + file, e);
        }
    }

    // ── Sections ───────────────────────────────────────────────────────────────

    private static void appendHeader(StringBuilder out, ExecutionState state) {
        String status = state.status() == null ? "UNKNOWN" : state.status().name();
        out.append("<header>\n");
        out.append("<h1>").append(escape(taskTypeOf(state))).append("</h1>\n");
        out.append("<p class=\"id\">").append(escape(idOf(state))).append("</p>\n");
        out.append("<p><span class=\"badge ").append(statusClass(state.status())).append("\">")
                .append(escape(status)).append("</span>");
        out.append(" <span class=\"muted\">")
                .append("attempt ").append(state.attempt())
                .append(" · ").append(state.retries()).append(" retr")
                .append(state.retries() == 1 ? "y" : "ies")
                .append("</span></p>\n");
        if (state.error() != null && !state.error().isBlank()) {
            out.append("<p class=\"error\">").append(escape(state.error())).append("</p>\n");
        }
        out.append("</header>\n");
    }

    private static void appendSummary(StringBuilder out, ExecutionState state, Timeline timeline) {
        out.append("<section class=\"summary\">\n<dl>\n");
        tile(out, "Model calls", String.valueOf(timeline.modelCalls()));
        tile(out, "Tool calls", String.valueOf(timeline.toolCalls()));
        tile(out, "Prompt tokens", String.valueOf(timeline.promptTokens()));
        tile(out, "Completion tokens", String.valueOf(timeline.completionTokens()));
        tile(out, "Cost", String.format(Locale.ROOT, "$%.6f", timeline.totalCostUsd()));
        tile(out, "Boundary latency", millis(timeline.totalLatencyMillis()));
        tile(out, "Wall clock", wallClock(state));
        tile(out, "Steps", String.valueOf(state.timeline().size()));
        out.append("</dl>\n</section>\n");
    }

    private static void tile(StringBuilder out, String label, String value) {
        out.append("<div><dt>").append(escape(label)).append("</dt><dd>")
                .append(escape(value)).append("</dd></div>\n");
    }

    private static void appendSteps(StringBuilder out, ExecutionState state) {
        out.append("<section class=\"steps\">\n<h2>Timeline</h2>\n");
        if (state.timeline().isEmpty()) {
            out.append("<p class=\"muted\">No steps recorded.</p>\n</section>\n");
            return;
        }
        // The table scrolls inside its own container so a long label or a wide payload never forces
        // the page itself to scroll sideways.
        out.append("<div class=\"scroll\">\n<table>\n<thead><tr>");
        out.append("<th>seq</th><th>kind</th><th>label</th><th class=\"num\">t+</th>")
                .append("<th class=\"num\">latency</th><th>detail</th>");
        out.append("</tr></thead>\n<tbody>\n");

        Instant start = state.startedAt();
        for (TimelineStep step : state.timeline()) {
            out.append("<tr>");
            out.append("<td class=\"num\">").append(step.seq()).append("</td>");
            out.append("<td><span class=\"kind ").append(kindClass(step.kind())).append("\">")
                    .append(escape(step.kind() == null ? "?" : step.kind().name())).append("</span></td>");
            out.append("<td>").append(escape(step.label() == null ? "" : step.label())).append("</td>");
            out.append("<td class=\"num\">").append(escape(offset(start, step.at()))).append("</td>");
            out.append("<td class=\"num\">")
                    .append(step.latencyMillis() > 0 ? escape(millis(step.latencyMillis())) : "")
                    .append("</td>");
            out.append("<td>").append(detail(step.detail())).append("</td>");
            out.append("</tr>\n");
        }
        out.append("</tbody>\n</table>\n</div>\n</section>\n");
    }

    private static void appendFooter(StringBuilder out) {
        out.append("<footer class=\"muted\">Folded from the execution's event log by Catalyst. ")
                .append("Every number above is a fold over the same events — there is no separate ")
                .append("metrics pipeline.</footer>\n");
    }

    // ── Values ─────────────────────────────────────────────────────────────────

    private static String idOf(ExecutionState state) {
        return state.id() == null ? "(unknown id)" : state.id().value();
    }

    private static String taskTypeOf(ExecutionState state) {
        return state.taskType() == null || state.taskType().isBlank() ? "(unknown task)" : state.taskType();
    }

    /** A recorded payload, rendered in a collapsed block so a big completion cannot swamp the page. */
    private static String detail(JsonNode node) {
        if (node == null || node.isNull()) return "";
        String text = node.toString();
        boolean elided = text.length() > MAX_DETAIL_CHARS;
        String shown = elided ? text.substring(0, MAX_DETAIL_CHARS) : text;
        StringBuilder sb = new StringBuilder();
        sb.append("<details><summary>").append(text.length()).append(" chars</summary><pre>")
                .append(escape(shown));
        if (elided) sb.append("\n… elided (").append(text.length() - MAX_DETAIL_CHARS).append(" more)");
        sb.append("</pre></details>");
        return sb.toString();
    }

    private static String offset(Instant start, Instant at) {
        if (start == null || at == null) return "";
        long ms = Duration.between(start, at).toMillis();
        return ms < 0 ? "" : "+" + millis(ms);
    }

    private static String millis(long ms) {
        if (ms < 1_000) return ms + " ms";
        return String.format(Locale.ROOT, "%.2f s", ms / 1_000.0);
    }

    private static String wallClock(ExecutionState state) {
        if (state.startedAt() == null) return "—";
        Instant end = state.endedAt();
        if (end == null) return "in flight";
        return millis(Duration.between(state.startedAt(), end).toMillis());
    }

    private static String statusClass(Status status) {
        if (status == null) return "unknown";
        return switch (status) {
            case COMPLETED -> "ok";
            case FAILED -> "bad";
            case CANCELLED -> "warn";
            default -> "running";
        };
    }

    private static String kindClass(TimelineStep.Kind kind) {
        if (kind == null) return "other";
        return switch (kind) {
            case MODEL -> "model";
            case TOOL -> "tool";
            case EFFECT -> "effect";
            case MEMORY_READ, MEMORY_WRITE -> "memory";
            case FAILED, CANCELLED -> "bad";
            case COMPLETED -> "ok";
            case RETRY, PAUSED, BRANCHED -> "warn";
            default -> "other";
        };
    }

    /**
     * Escapes the five characters that can break out of HTML text or an attribute. Applied to every
     * interpolated value without exception — log content is data, never markup.
     */
    static String escape(String raw) {
        if (raw == null) return "";
        StringBuilder sb = new StringBuilder(raw.length() + 16);
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '&' -> sb.append("&amp;");
                case '<' -> sb.append("&lt;");
                case '>' -> sb.append("&gt;");
                case '"' -> sb.append("&quot;");
                case '\'' -> sb.append("&#39;");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String css() {
        return """
                :root { color-scheme: light dark;
                  --bg:#fff; --fg:#1a1a1a; --muted:#6b7280; --line:#e5e7eb; --panel:#f9fafb; }
                @media (prefers-color-scheme: dark) {
                  :root { --bg:#0f1115; --fg:#e6e6e6; --muted:#9aa1ab; --line:#252a33; --panel:#161a21; }
                }
                * { box-sizing: border-box; }
                body { margin:0; padding:2rem 1.25rem; background:var(--bg); color:var(--fg);
                  font:15px/1.55 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
                  max-width:70rem; margin-inline:auto; }
                h1 { font-size:1.4rem; margin:0 0 .2rem; }
                h2 { font-size:1rem; text-transform:uppercase; letter-spacing:.06em;
                  color:var(--muted); margin:2rem 0 .6rem; }
                .id { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; color:var(--muted);
                  margin:0 0 .6rem; font-size:.85rem; }
                .muted { color:var(--muted); }
                .error { background:var(--panel); border-left:3px solid #dc2626; padding:.6rem .8rem;
                  font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:.85rem;
                  white-space:pre-wrap; overflow-wrap:anywhere; }
                .badge { display:inline-block; padding:.15rem .55rem; border-radius:999px;
                  font-size:.75rem; font-weight:600; letter-spacing:.04em; }
                .badge.ok { background:#dcfce7; color:#166534; }
                .badge.bad { background:#fee2e2; color:#991b1b; }
                .badge.warn { background:#fef3c7; color:#92400e; }
                .badge.running,.badge.unknown { background:#e0e7ff; color:#3730a3; }
                .summary dl { display:grid; gap:.75rem; margin:1.5rem 0 0;
                  grid-template-columns:repeat(auto-fit,minmax(9rem,1fr)); }
                .summary div { background:var(--panel); border:1px solid var(--line);
                  border-radius:.5rem; padding:.6rem .75rem; }
                .summary dt { font-size:.72rem; text-transform:uppercase; letter-spacing:.05em;
                  color:var(--muted); }
                .summary dd { margin:.15rem 0 0; font-size:1.15rem;
                  font-variant-numeric:tabular-nums; }
                .scroll { overflow-x:auto; }
                table { border-collapse:collapse; width:100%; font-size:.88rem; }
                th,td { text-align:left; padding:.45rem .6rem; border-bottom:1px solid var(--line);
                  vertical-align:top; }
                th { font-size:.72rem; text-transform:uppercase; letter-spacing:.05em;
                  color:var(--muted); font-weight:600; }
                td.num,th.num { text-align:right; font-variant-numeric:tabular-nums;
                  white-space:nowrap; }
                .kind { font-size:.7rem; font-weight:600; letter-spacing:.04em;
                  padding:.1rem .4rem; border-radius:.25rem; white-space:nowrap; }
                .kind.model { background:#e0e7ff; color:#3730a3; }
                .kind.tool { background:#cffafe; color:#155e75; }
                .kind.effect { background:#f3e8ff; color:#6b21a8; }
                .kind.memory { background:#ecfccb; color:#3f6212; }
                .kind.ok { background:#dcfce7; color:#166534; }
                .kind.bad { background:#fee2e2; color:#991b1b; }
                .kind.warn { background:#fef3c7; color:#92400e; }
                .kind.other { background:var(--panel); color:var(--muted); }
                details pre { margin:.4rem 0 0; padding:.5rem; background:var(--panel);
                  border-radius:.35rem; font-size:.78rem; white-space:pre-wrap;
                  overflow-wrap:anywhere; max-height:22rem; overflow:auto; }
                summary { cursor:pointer; color:var(--muted); font-size:.78rem; }
                footer { margin-top:2.5rem; padding-top:1rem; border-top:1px solid var(--line);
                  font-size:.8rem; }
                """;
    }
}
