package common;

/**
 * notice GreedyJoin only support skip-till-any-match
 * currently OrderJoin and GreedyJoin can not process complex event pattern
 */
public enum MatchEngine {
    NFA
}
// remove OrderJoin, GreedyJoin
