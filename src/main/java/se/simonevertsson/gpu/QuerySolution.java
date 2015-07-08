package se.simonevertsson.gpu;

import se.simonevertsson.query.AliasDictionary;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by simon.evertsson on 2015-07-07.
 */
public class QuerySolution {

    private List<Map.Entry<String, Integer>> solution;

    public QuerySolution(List<Map.Entry<String, Integer>> solutionElements) {
        this.solution = solutionElements;
    }

    public void sort(AliasDictionary aliasDictionary) {
        List<Map.Entry<String, Integer>> sortedSolution = this.solution;

        for(Map.Entry<String, Integer> solutionElement : this.solution) {
            String alias = solutionElement.getKey();
            sortedSolution.set(aliasDictionary.getIdForAlias(alias), solutionElement);
        }
    }

    public void print() {
        StringBuilder builder = new StringBuilder();
        for()
        builder.append()
    }
}
