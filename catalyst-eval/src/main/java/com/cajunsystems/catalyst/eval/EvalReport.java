package com.cajunsystems.catalyst.eval;

import com.cajunsystems.catalyst.engine.Trajectory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The results of an evaluation: every (case, candidate) pair, in the order given, and a summary per
 * candidate. Deterministic for a given log and candidates' outputs, so two reports diff cleanly.
 */
public record EvalReport(List<String> candidates, List<CaseResult> results) {

    public EvalReport {
        candidates = List.copyOf(candidates);
        results = List.copyOf(results);
    }

    /** The results for one candidate, in case order. */
    public List<CaseResult> resultsFor(String candidate) {
        return results.stream().filter(r -> r.candidate().equals(candidate)).toList();
    }

    /** Aggregates for one candidate. */
    public Summary summary(String candidate) {
        List<CaseResult> rs = resultsFor(candidate);
        Map<CaseResult.Outcome, Integer> outcomes = new LinkedHashMap<>();
        for (CaseResult.Outcome o : CaseResult.Outcome.values()) outcomes.put(o, 0);
        rs.forEach(r -> outcomes.merge(r.outcome(), 1, Integer::sum));

        Set<String> scorers = new LinkedHashSet<>();
        rs.forEach(r -> r.scores().forEach(s -> scorers.add(s.scorer())));
        Map<String, Double> passRate = new LinkedHashMap<>();
        Map<String, Double> meanValue = new LinkedHashMap<>();
        for (String scorer : scorers) {
            List<Score> ss = new ArrayList<>();
            rs.forEach(r -> r.scores().stream().filter(s -> s.scorer().equals(scorer)).forEach(ss::add));
            passRate.put(scorer, ss.stream().filter(Score::pass).count() / (double) ss.size());
            meanValue.put(scorer, ss.stream().mapToDouble(Score::value).average().orElse(Double.NaN));
        }

        double baselineUsd = 0, candidateUsd = 0;
        long baselineTokens = 0, candidateTokens = 0;
        for (CaseResult r : rs) {
            if (r.sample() == null) continue;
            Trajectory b = r.sample().baselineTrajectory();
            Trajectory c = r.sample().candidateTrajectory();
            baselineUsd += b.cost().usd();
            candidateUsd += c.cost().usd();
            baselineTokens += b.cost().totalTokens();
            candidateTokens += c.cost().totalTokens();
        }
        long passedAll = rs.stream().filter(CaseResult::passed).count();
        return new Summary(candidate, rs.size(), passedAll, outcomes, passRate, meanValue,
                baselineUsd, candidateUsd, baselineTokens, candidateTokens);
    }

    /**
     * Per-candidate aggregates. Costs and tokens cover completed cases only, both sides, so they
     * compare like with like.
     */
    public record Summary(String candidate, int cases, long passedAllScorers,
                          Map<CaseResult.Outcome, Integer> outcomes,
                          Map<String, Double> passRate, Map<String, Double> meanValue,
                          double baselineUsd, double candidateUsd, long baselineTokens, long candidateTokens) {
    }

    /** A plain-text table: one block per candidate, then the cases that did not pass. */
    public String pretty() {
        StringBuilder sb = new StringBuilder();
        for (String candidate : candidates) {
            Summary s = summary(candidate);
            sb.append("candidate ").append(candidate).append(": ")
                    .append(s.passedAllScorers()).append('/').append(s.cases()).append(" passed all scorers\n");
            sb.append("  outcomes:");
            s.outcomes().forEach((o, n) -> { if (n > 0) sb.append(' ').append(o).append('=').append(n); });
            sb.append('\n');
            s.passRate().forEach((scorer, rate) -> sb.append(String.format("  %-16s pass %5.1f%%  mean %.3f%n",
                    scorer, rate * 100, s.meanValue().get(scorer))));
            sb.append(String.format("  tokens %d -> %d, cost $%.6f -> $%.6f%n",
                    s.baselineTokens(), s.candidateTokens(), s.baselineUsd(), s.candidateUsd()));
            for (CaseResult r : resultsFor(candidate)) {
                if (r.passed()) continue;
                sb.append("  ✗ ").append(r.caseId().value()).append(' ').append(r.outcome());
                if (r.error() != null) sb.append(": ").append(r.error());
                r.scores().stream().filter(sc -> !sc.pass())
                        .forEach(sc -> sb.append(" [").append(sc.scorer()).append(": ").append(sc.detail()).append(']'));
                sb.append('\n');
            }
        }
        return sb.toString();
    }
}
