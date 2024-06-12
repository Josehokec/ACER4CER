package store;

/**
 * RID = <page, offset>
 * In fact, if the size of a page does not exceed 32KB (15 bits),
 * it is best to set the offset type to short,
 * which will improve the performance of the IntervalScanMethod and NaiveIndexMethod.
 * here we set it as future optimization points
 */
public record RID(int page, short offset) implements Comparable<RID> {

    @Override
    public String toString() {
        return "page: " + page + " offset: " + offset;
    }

    @Override
    public int compareTo(RID o) {
        if (this.page() != o.page()) {
            return this.page() - o.page();
        } else {
            return this.offset() - o.offset();
        }
    }

    public long getLongKey(){
        return (page << 16) + offset;
    }
}

