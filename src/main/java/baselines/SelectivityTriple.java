package baselines;

/**
 * if icPos = -1, then it means we choose type
 * else if icPos = i, it means we choose independent constraints
 * @param varId         variable id
 * @param icPos         attribute id [type id is -1]
 * @param selectivity   selectivity
 */
public record SelectivityTriple(int varId, int icPos, double selectivity) { }
