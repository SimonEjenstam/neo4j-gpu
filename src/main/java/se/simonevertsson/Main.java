package se.simonevertsson;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.experiments.CypherQueryRunner;
import se.simonevertsson.experiments.GpuQueryRunner;
import se.simonevertsson.gpu.QuerySolution;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by simon on 2015-05-12.
 */
public class Main {

    public static final String TEST_DB_PATH = "target/foo";
    public static final String DR_WHO_DB_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j\\drwho";
    public static final String DR_WHO_DB_CONFIG_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j";
//    public static final String EXPERIMENT_QUERY =
//            "MATCH (a1),(b2),(a3),(c4)" +
//            "WHERE (a3)<--(a1)-->(b2) AND (a3)<--(b2)-->(c4)<--(a3)" +
//            "RETURN a1, b2, a3, c4";


//    public static final String EXPERIMENT_QUERY =
//            "MATCH \n" +
//                    "\t(a1)-->(b2), \n" +
//                    "\t(a1)-->(a3),\n" +
//                    "\t(b2)-->(a3),\n" +
//                    "\t(b2)-->(c4),\n" +
//                    "\t(a3)-->(c4)\n" +
//                    "RETURN id(a1), id(b2), id(a3), id(c4);";


    public static final String EXPERIMENT_QUERY =
            "MATCH (A1)-->(B2), (A1)-->(A3), (B2)-->(A3), (B2)-->(C4), (A3)-->(C4) RETURN A1, B2, A3, C4;";

    public static final String EXPERIMENT_QUERY_PREFIX =
            "MATCH (A1)-->(B2), (A1)-->(A3), (B2)-->(A3), (B2)-->(C4), (A3)-->(C4)";

    public static final String EXPERIMENT_QUERY_SUFFIX =
            " RETURN A1, B2, A3, C4;";


//    public static final String EXPERIMENT_QUERY =
//            "MATCH " +
//                    "(a)-->(b)," +
//                    "(a)-->(c)," +
//                    "(b)-->(c)" +
//                    "RETURN a,b,c";

//    public static final String EXPERIMENT_QUERY =
//            "MATCH " +
//                    "(a1)-->(a2)" +
//                    "RETURN count(*)";

    public static void main(String[] args) throws IOException {
        /* Setup database */
//        DatabaseService databaseService = new DatabaseService(TEST_DB_PATH);
//        ExperimentSetup experimentSetup = new ExperimentSetup();
//        experimentSetup.fillDatabaseWithSmallDrWhoTestData(databaseService.getGraphDatabase());

        DatabaseService databaseService = new DatabaseService(DR_WHO_DB_PATH, DR_WHO_DB_CONFIG_PATH);



        /* Cypher query run */
        CypherQueryRunner cypherQueryRunner = new CypherQueryRunner();
        List<QuerySolution> cypherQueryResult = cypherQueryRunner.runCypherQuery(databaseService, EXPERIMENT_QUERY_PREFIX + EXPERIMENT_QUERY_SUFFIX);
        for(QuerySolution solution : cypherQueryResult) {
            solution.print();
        }
//        Set<String> uniqueCypherResults = new HashSet<String>(cypherQueryResult);
//        System.out.println("Number of unique rows: " + uniqueCypherResults.size());
//        validateSolutions(databaseService, cypherQueryResult);
//        System.out.println(cypherQueryResult);

        /* GPU query run */
        GpuQueryRunner gpuQueryRunner = new GpuQueryRunner();
        List<String> results = gpuQueryRunner.runGpuQuery(databaseService);

        Set<String> uniqueResults = new HashSet<String>(results);
        System.out.println("Unique results count: " + uniqueResults.size());

        /* Validate solutions */
        validateSolutions(databaseService, results);

        Set<String> newResults = new HashSet<String>(uniqueResults);

        newResults.removeAll(uniqueCypherResults);
        System.out.println(newResults);
        System.out.println("New results count: " + newResults.size());

        /* Tear down database */
        databaseService.deleteData();
        databaseService.shutdown();
        System.out.println("foo");
    }


    private static void validateSolutions(DatabaseService databaseService, List<String> results) {
        Transaction tx = databaseService.beginTx();
        int invalidSolutions = 0;
        for(String result : results) {
            Result queryResult = databaseService.excuteCypherQuery(result);
            if(!queryResult.hasNext()) {
                System.out.println("Solution below yielded no results (invalid)");
                System.out.println(result);
                invalidSolutions++;
            }
            queryResult.close();
        }
        tx.success();
        if(invalidSolutions == 0) {
            System.out.println("All solutions were valid");
        } else {
            System.out.println(invalidSolutions + " solutions were invalid");
        }
    }

}
