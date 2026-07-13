package com.cajunsystems.catalyst.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The step-by-step difference between two {@link Trajectory}s — an original run and a branch of it
 * (spec §7). Only "work" steps (model calls, tool calls, effects, memory) are compared; lifecycle
 * bookkeeping (created/started/branched/completed) is ignored so the diff reflects what the AI work
 * actually did differently. Steps are aligned by position and classified.
 */
public record TrajectoryDiff(List<Entry> entries) {

    public TrajectoryDiff {
        entries = entries == null ? List.of() : List.copyOf(entries);
    }

    public enum Change { SAME, CHANGED, ADDED, REMOVED }

    /** One aligned position: {@code base}/{@code fork} may be null for ADDED/REMOVED. */
    public record Entry(int index, Change change, TimelineStep base, TimelineStep fork) {}

    static TrajectoryDiff between(Trajectory base, Trajectory fork) {
        List<TimelineStep> a = workSteps(base);
        List<TimelineStep> b = workSteps(fork);
        List<Entry> out = new ArrayList<>();
        int n = Math.max(a.size(), b.size());
        for (int i = 0; i < n; i++) {
            TimelineStep left = i < a.size() ? a.get(i) : null;
            TimelineStep right = i < b.size() ? b.get(i) : null;
            Change change;
            if (left != null && right != null) {
                change = sameContent(left, right) ? Change.SAME : Change.CHANGED;
            } else if (left == null) {
                change = Change.ADDED;
            } else {
                change = Change.REMOVED;
            }
            out.add(new Entry(i, change, left, right));
        }
        return new TrajectoryDiff(out);
    }

    /** Whether any position differs. */
    public boolean hasChanges() {
        return entries.stream().anyMatch(e -> e.change() != Change.SAME);
    }

    public long changedCount() {
        return entries.stream().filter(e -> e.change() != Change.SAME).count();
    }

    /** A human-readable rendering, one line per aligned step. */
    public String pretty() {
        StringBuilder sb = new StringBuilder();
        for (Entry e : entries) {
            TimelineStep ref = e.base() != null ? e.base() : e.fork();
            char mark = switch (e.change()) {
                case SAME -> ' ';
                case CHANGED -> '~';
                case ADDED -> '+';
                case REMOVED -> '-';
            };
            sb.append(mark).append(" [").append(e.index()).append("] ")
                    .append(ref.kind()).append("  ").append(e.change());
            if (e.change() == Change.CHANGED) {
                sb.append("  base=").append(label(e.base())).append("  fork=").append(label(e.fork()));
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static String label(TimelineStep s) {
        Object detail = s.detail() != null ? s.detail() : s.label();
        return String.valueOf(detail);
    }

    private static boolean sameContent(TimelineStep a, TimelineStep b) {
        return a.kind() == b.kind()
                && Objects.equals(a.label(), b.label())
                && Objects.equals(a.detail(), b.detail());
    }

    private static List<TimelineStep> workSteps(Trajectory t) {
        List<TimelineStep> out = new ArrayList<>();
        for (TimelineStep s : t.steps()) {
            if (isWork(s.kind())) out.add(s);
        }
        return out;
    }

    private static boolean isWork(TimelineStep.Kind kind) {
        return switch (kind) {
            case PROMPT, MODEL, TOOL, EFFECT, MEMORY_READ, MEMORY_WRITE -> true;
            default -> false;
        };
    }
}
