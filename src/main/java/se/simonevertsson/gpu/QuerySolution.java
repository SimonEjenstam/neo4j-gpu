package se.simonevertsson.gpu;

import org.neo4j.graphdb.Node;
import se.simonevertsson.Main;
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


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
//        for(Map.Entry<String, Node> solutionElement : solution) {
//            builder.append(solutionElement.getKey() + ":");
//            builder.append(solutionElement.getValue().getId());
//            builder.append(", ");
//        }

        builder.append(Main.EXPERIMENT_QUERY_PREFIX);
        builder.append(" WHERE ");
        boolean firstReturnNode = true;
        for ( Map.Entry<String, Integer> solutionElement : this.solution )
        {
            if(!firstReturnNode) {
                builder.append(" AND ");
            } else {
                firstReturnNode = false;
            }
            builder.append("id(" + solutionElement.getKey() + ")=" + solutionElement.getValue());
        }
        builder.append(Main.EXPERIMENT_QUERY_SUFFIX);
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof QuerySolution) {
            QuerySolution that = (QuerySolution) obj;
            if(this.solution.size() != that.solution.size()) {
                return false;
            }

            for(Map.Entry<String, Integer> thisSolutionElement : this.solution) {

                boolean matchFound = false;
                for(Map.Entry<String, Integer> thatSolutionElement : that.solution) {
                    if(thisSolutionElement.getKey().compareTo(thatSolutionElement.getKey()) == 0) {
                        if(thisSolutionElement.getValue() != thatSolutionElement.getValue()) {
                            return false;
                        }
                    }
                }

            }
        }
        return true;
    }
}
