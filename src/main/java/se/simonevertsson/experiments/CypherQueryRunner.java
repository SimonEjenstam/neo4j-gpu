package se.simonevertsson.experiments;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.gpu.QuerySolution;

import java.util.*;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class CypherQueryRunner {

    public List<QuerySolution> runCypherQueryForSolutions(DatabaseService dbService, String query) {
        System.out.println("Starting CypherQueryRunner");
        long tick = System.currentTimeMillis();


        List<QuerySolution> querySolutions;
        try(Transaction tx = dbService.beginTx()) {
            Result result = dbService.excuteCypherQuery(query);

            long tock = System.currentTimeMillis();
            System.out.println("CypherQueryRunner runtime: " + (tock - tick) + "ms");

            querySolutions = handleResult(result);
            tx.success();
        }


        return querySolutions;
    }

    private List<QuerySolution> handleResult(Result result) {
        List<Map.Entry<String, Integer>> solutionElements;
        int resultCount = 0;
        List<String> columns = result.columns();
        ArrayList<QuerySolution> results = new ArrayList<QuerySolution>();
        while ( result.hasNext() )
        {
            solutionElements = new ArrayList<Map.Entry<String, Integer>>();

            Map<String,Object> row = result.next();
            for ( String key : result.columns() )
            {
                Node resultNode = ((Node) row.get(key));
                Map.Entry<String, Integer> solutionElement = new AbstractMap.SimpleEntry<String, Integer>(key, (int) resultNode.getId());
                solutionElements.add(solutionElement);
            }
            QuerySolution querySolution = new QuerySolution(solutionElements);
            results.add(querySolution);
//            builder  = new StringBuilder();
//            resultCount++;
//            builder.append(Main.EXPERIMENT_QUERY_PREFIX);
//            builder.append(" WHERE ");
//            Map<String,Object> row = result.next();
//            boolean firstReturnNode = true;
//            for ( Map.Entry<String,Object> column : row.entrySet() )
//            {
//                if(!firstReturnNode) {
//                    builder.append(" AND ");
//                } else {
//                    firstReturnNode = false;
//                }
//                builder.append("id(" + column.getKey() + ")=" + column.getValue().toString().replaceAll("\\D+",""));
//            }
//            builder.append(Main.EXPERIMENT_QUERY_SUFFIX);
//            results.add(builder.toString());
        }
        result.close();
       return results;
    }
}
