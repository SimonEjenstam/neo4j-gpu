package se.simonevertsson.experiments;

import org.neo4j.graphdb.Result;
import se.simonevertsson.db.DatabaseService;

import java.util.Map;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class CypherQueryRunner {

    public String runCypherQuery(DatabaseService dbService, String query) {
        System.out.println("Starting CypherQueryRunner");
        long tick = System.currentTimeMillis();
        Result result = dbService.excuteCypherQuery(query);
        long tock = System.currentTimeMillis();

        System.out.println("CypherQueryRunner runtime: " + (tock-tick) + "ms");
        return handleResult(result);
    }

    private String handleResult(Result result) {
        StringBuilder builder  = new StringBuilder();
        int resultCount = 0;
        while ( result.hasNext() )
        {
            resultCount++;
            Map<String,Object> row = result.next();
            for ( Map.Entry<String,Object> column : row.entrySet() )
            {
                builder.append( column.getKey() + ": " + column.getValue() + "; " );
            }
            builder.append("\n");
        }
        builder.append("Solution count: " + resultCount + "\n");
       return builder.toString();
    }
}
