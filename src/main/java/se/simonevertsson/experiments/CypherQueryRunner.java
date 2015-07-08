package se.simonevertsson.experiments;

import org.neo4j.graphdb.Result;
import se.simonevertsson.Main;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.gpu.QuerySolution;

import java.util.*;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class CypherQueryRunner {

    public List<QuerySolution> runCypherQuery(DatabaseService dbService, String query) {
        System.out.println("Starting CypherQueryRunner");
        long tick = System.currentTimeMillis();
        Result result = dbService.excuteCypherQueryWithinTransaction(query);
        long tock = System.currentTimeMillis();



        System.out.println("CypherQueryRunner runtime: " + (tock-tick) + "ms");
        return handleResult(result);
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
            for ( Map.Entry<String,Object> column : row.entrySet() )
            {
                Map.Entry<String, Integer> solutionElement = new AbstractMap.SimpleEntry<String, Integer>(column.getKey(), (Integer) column.getValue());
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
       return results;
    }
}
