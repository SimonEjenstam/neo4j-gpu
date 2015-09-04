package se.simonevertsson;

import org.neo4j.graphdb.*;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.experiments.*;
import se.simonevertsson.gpu.QueryResult;
import se.simonevertsson.gpu.QuerySolution;
import se.simonevertsson.query.QueryGraph;

import java.io.IOException;
import java.util.*;

/**
 * Created by simon on 2015-05-12.
 */
public class Main {

    public static final String TEST_DB_PATH = "target/foo";
//    public static final String DB_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j\\cineasts_12k_movies_50k_actors";
    public static final String DB_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j\\yeast";
//    public static final String DB_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j\\drwho";
    public static final String DR_WHO_DB_CONFIG_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j";


    public static void main(String[] args) throws IOException {
        /* Setup database */
        DatabaseService databaseService = new DatabaseService(DB_PATH, DR_WHO_DB_CONFIG_PATH);





//        ArrayList<Node> allNodes = databaseService.getAllNodes();
//        ExperimentQueryGraphGenerator experimentQueryGraphGenerator = new ExperimentQueryGraphGenerator(allNodes, 5, 2);
//        QueryGraph queryGraph = experimentQueryGraphGenerator.generate();

//        for(int i = 0; i < 5; i++) {

            ArrayList<Node> allNodes = databaseService.getAllNodes();
            MultipleExperimentQueryGraphGenerator experimentQueryGraphGenerator = new MultipleExperimentQueryGraphGenerator(allNodes, 8, 1, 2);
            List<QueryGraph> queryGraphs = experimentQueryGraphGenerator.generate();

            //        RandomQueryGraphGenerator queryGraphGenerator = new RandomQueryGraphGenerator(databaseService);
            //        QueryGraph queryGraph = queryGraphGenerator.generate(4, 2);


            for (QueryGraph queryGraph : queryGraphs) {
                System.out.println("*********** NEW QUERY **********");
                System.out.println("Querying with query:");
                System.out.println(queryGraph.toCypherQueryString());

                 /* GPU query run */
                GpuQueryRunner gpuQueryRunner = new GpuQueryRunner();
                QueryResult gpuQueryResult = gpuQueryRunner.runGpuQuery(databaseService, queryGraph);
                System.out.println("GPU Data conversion execution time: " + gpuQueryResult.getConversionExecutionTime() + "ms");
                System.out.println("GPU query execution time: " + gpuQueryResult.getQueryExecutionTime() + "ms");

                Set<String> uniqueResults = new HashSet<String>();
                for (QuerySolution solution : gpuQueryResult.getQuerySolutions()) {
                    uniqueResults.add(solution.toString());
                }
                System.out.println("Number of solutions: " + gpuQueryResult.getQuerySolutions().size());
                System.out.println("Unique results count: " + uniqueResults.size());

                //        /* Validate solutions */
                //        validateQuerySolutions(databaseService, uniqueResults);

                /* Cypher query run */
                CypherQueryRunner cypherQueryRunner = new CypherQueryRunner();
                System.out.println("Starting Cypher query.");
                QueryResult cypherQueryResult = cypherQueryRunner.runCypherQueryForSolutions(databaseService, queryGraph);
                System.out.println("Cypher query execution time: " + cypherQueryResult.getQueryExecutionTime() + "ms");

                Set<String> uniqueCypherResults = new HashSet<String>();
                for (QuerySolution solution : cypherQueryResult.getQuerySolutions()) {
                    uniqueCypherResults.add(solution.toString());
                }
                System.out.println("Cypher solution count: " + cypherQueryResult.getQuerySolutions().size());
                System.out.println("Number of unique rows: " + uniqueCypherResults.size());
                System.out.println("Speedup: " + ((double) cypherQueryResult.getTotalExecutionTime() / (double) gpuQueryResult.getTotalExecutionTime()) + "x");

                System.out.println("Speedup excluding conversion time: " + ((double) cypherQueryResult.getTotalExecutionTime() / (double) gpuQueryResult.getQueryExecutionTime()) + "x");

                //        validateQuerySolutions(databaseService, uniqueCypherResults);
                //
                //
                //
                //
                //        compareQuerySolutions(cypherQueryResult, results);

            }
//        }

        /* Tear down database */
        System.out.println("Terminating database connection...");
        databaseService.shutdown();
        System.out.println("Database connection terminated.");
    }

    private static void compareQuerySolutions(List<QuerySolution> cypherQueryResult, List<QuerySolution> gpuQueryResult) {
        List<QuerySolution> cypherQueryResultClone = new ArrayList<>(cypherQueryResult);
        for(QuerySolution gpuSolution : gpuQueryResult) {
            Iterator<QuerySolution> itr = cypherQueryResultClone.iterator();
            while(itr.hasNext()) {
                QuerySolution cypherSolution = itr.next();
                if(gpuSolution.equals(cypherSolution)) {
                    itr.remove();
                    break;
                }
            }
        }

        for(QuerySolution missingGpuSolution : cypherQueryResultClone) {
            System.out.println("GPU result does not contain Cypher solution below");
            System.out.println(missingGpuSolution);
        }


        List<QuerySolution> gpuQueryResultClone = new ArrayList<>(gpuQueryResult);
        for(QuerySolution cypherSolution : cypherQueryResult) {
            Iterator<QuerySolution> itr = gpuQueryResultClone.iterator();
            while(itr.hasNext()) {
                QuerySolution gpuSolution = itr.next();
                if(cypherSolution.equals(gpuSolution)) {
                    itr.remove();
                    break;
                }
            }

        }

        for(QuerySolution missingCypherSolution : gpuQueryResultClone) {
            System.out.println("Cypher result does not contain GPU solution below");
            System.out.println(missingCypherSolution);
        }

        System.out.println("Missing Cypher solution count: " + cypherQueryResultClone.size());
        System.out.println("Missing GPU solution count: " + gpuQueryResultClone.size());
    }

    private static void validateQuerySolutions(DatabaseService databaseService, Set<String> results) {
        try(Transaction tx = databaseService.beginTx()) {
            int invalidSolutions = 0;
            for (String querySolutionQuery : results) {
                Result queryResult = databaseService.excuteCypherQuery(querySolutionQuery);
                if (!queryResult.hasNext()) {
                    System.out.println(querySolutionQuery);
                    invalidSolutions++;
                }
                queryResult.close();
            }

            if (invalidSolutions == 0) {
                System.out.println("All solutions were valid");
            } else {
                System.out.println(invalidSolutions + " solutions were invalid");
            }
            tx.success();
        }
    }

}
