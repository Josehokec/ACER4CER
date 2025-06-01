package acer;

import automaton.Tuple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NegationProcessor {
    private List<Tuple> firstQueryTuples;
    private List<Tuple> secondQueryTuples;

    public NegationProcessor(List<Tuple> firstQueryTuples, List<Tuple> secondQueryTuples) {
        this.firstQueryTuples = firstQueryTuples;
        this.secondQueryTuples = secondQueryTuples;
    }

    public List<Tuple> getResult(int projectionPos){
        List<Tuple> finalResults = new ArrayList<>(firstQueryTuples.size());
        Set<String> discardMatch = new HashSet<>(secondQueryTuples.size() * 2);
        for(Tuple tuple : secondQueryTuples){
            discardMatch.add(tuple.projectionExclude(projectionPos));
        }

        for(Tuple tuple : firstQueryTuples){
            boolean shouldDiscard = discardMatch.contains(tuple.getKey());
            if(!shouldDiscard){
                finalResults.add(tuple);
            }
        }
        return finalResults;
    }

}
