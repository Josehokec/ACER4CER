package condition;

/**
 * this a quad used for independent constraints' query
 * @param idx - i-th range bitmap
 * @param mark - 1 means >= min; 2 means <= max; 3 means [min, max]
 * @param min - minimum value
 * @param max - maximum value
 */
public record ICQueryQuad(int idx, int mark, long min, long max){}