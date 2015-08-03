package se.simonevertsson.gpu;

import se.simonevertsson.query.AliasDictionary;
import se.simonevertsson.query.QueryGraph;

import java.util.List;
import java.util.Map;

/**
 * Created by simon.evertsson on 2015-07-07.
 */
public class QuerySolution {

    private List<Map.Entry<String, Integer>> solutionElements;

    private QueryGraph queryGraph;

    public QuerySolution(QueryGraph queryGraph, List<Map.Entry<String, Integer>> solutionElements) {
        this.queryGraph = queryGraph;
        this.solutionElements = solutionElements;
    }

    public void sort(AliasDictionary aliasDictionary) {
        List<Map.Entry<String, Integer>> sortedSolution = this.solutionElements;

        for(Map.Entry<String, Integer> solutionElement : this.solutionElements) {
            String alias = solutionElement.getKey();
            sortedSolution.set(aliasDictionary.getIdForAlias(alias), solutionElement);
        }
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
//        for(Map.Entry<String, Node> solutionElement : solutionElements) {
//            builder.append(solutionElement.getKey() + ":");
//            builder.append(solutionElement.getValue().getId());
//            builder.append(", ");
//        }

//        builder.append(Main.EXPERIMENT_QUERY_PREFIX);
//        builder.append(" WHERE ");
//        boolean firstReturnNode = true;
//        for ( Map.Entry<String, Integer> solutionElement : this.solutionElements )
//        {
//            if(!firstReturnNode) {
//                builder.append(" AND ");
//            } else {
//                firstReturnNode = false;
//            }
//            builder.append("id(" + solutionElement.getKey() + ")=" + solutionElement.getValue());
//        }
//        builder.append(Main.EXPERIMENT_QUERY_SUFFIX);

        builder.append(this.queryGraph.toCypherQueryStringPrefix());
        builder.append(" WHERE ");
        boolean firstReturnNode = true;
        for ( Map.Entry<String, Integer> solutionElement : this.solutionElements)
        {
            if(!firstReturnNode) {
                builder.append(" AND ");
            } else {
                firstReturnNode = false;
            }
            builder.append("id(" + solutionElement.getKey() + ")=" + solutionElement.getValue());
        }
        builder.append(this.queryGraph.toCypherQueryStringSuffix());
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        boolean result = false;
        if(obj instanceof QuerySolution) {
            QuerySolution that = (QuerySolution) obj;
            if(this.solutionElements.size() == that.solutionElements.size()) {
                for (Map.Entry<String, Integer> thisSolutionElement : this.solutionElements) {
                    boolean matchFound = false;
                    for (Map.Entry<String, Integer> thatSolutionElement : that.solutionElements) {
                        if (thisSolutionElement.getKey().compareTo(thatSolutionElement.getKey()) == 0) {
                            if (thisSolutionElement.getValue().intValue() == thatSolutionElement.getValue().intValue()) {
                                matchFound = true;
                                break;
                            }
                        }
                    }
                    if(!matchFound) {
                        return false;
                    }
                }
                result = true;
            }
        }
        return result;
    }
}
