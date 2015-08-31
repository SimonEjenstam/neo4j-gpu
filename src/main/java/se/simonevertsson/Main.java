package se.simonevertsson;

import org.neo4j.graphdb.*;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.experiments.CypherQueryRunner;
import se.simonevertsson.experiments.ExperimentQueryGraphGenerator;
import se.simonevertsson.experiments.GpuQueryRunner;
import se.simonevertsson.gpu.QuerySolution;
import se.simonevertsson.query.QueryGraph;

import java.io.IOException;
import java.util.*;

/**
 * Created by simon on 2015-05-12.
 */
public class Main {

    public static final String TEST_DB_PATH = "target/foo";
    public static final String DB_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j\\cineasts_12k_movies_50k_actors";
//    public static final String DB_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j\\drwho";
    public static final String DR_WHO_DB_CONFIG_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j";


    public static void main(String[] args) throws IOException {
        /* Setup database */
//        DatabaseService databaseService = new DatabaseService(TEST_DB_PATH);
//        ExperimentSetup experimentSetup = new ExperimentSetup();
//        experimentSetup.fillDatabaseWithSmallDrWhoTestData(databaseService.getGraphDatabase());

        DatabaseService databaseService = new DatabaseService(DB_PATH, DR_WHO_DB_CONFIG_PATH);


        ArrayList<Node> allNodes = databaseService.getAllNodes();

        ExperimentQueryGraphGenerator experimentQueryGraphGenerator = new ExperimentQueryGraphGenerator(allNodes, 7, 4, 2, 2);
//        ExperimentQueryGraphGenerator experimentQueryGraphGenerator = new ExperimentQueryGraphGenerator(allNodes, 5, 4, 2, 2);
        QueryGraph queryGraph = experimentQueryGraphGenerator.generate();
//        QueryGraph queryGraph = QueryGraphGenerator.generateLabeledTriangleMockQueryGraph();
        System.out.println("Querying with query:");
        System.out.println(queryGraph.toCypherQueryString());

//        QueryGraph dataGraph = QueryGraphGenerator.generateFailingDataGraph();
//        QueryGraph queryGraph = QueryGraphGenerator.generateFailingQueryGraph();
//        System.out.println("Querying with query:");
//        System.out.println(queryGraph.toCypherQueryString());

         /* GPU query run */
        GpuQueryRunner gpuQueryRunner = new GpuQueryRunner();
//      List<QuerySolution> results = gpuQueryRunner.runGpuQuery(databaseService);
        List<QuerySolution> results = gpuQueryRunner.runGpuQuery(databaseService, queryGraph);
//        List<QuerySolution> results = gpuQueryRunner.runGpuQuery(dataGraph, queryGraph);

        Set<String> uniqueResults = new HashSet<String>();
        for(QuerySolution solution : results) {
//            System.out.println(solution);
            uniqueResults.add(solution.toString());
        }
        System.out.println("Number of solutions: " + results.size());
        System.out.println("Unique results count: " + uniqueResults.size());

//        /* Validate solutions */
//        validateQuerySolutions(databaseService, uniqueResults);

        /* Cypher query run */
        CypherQueryRunner cypherQueryRunner = new CypherQueryRunner();

//        List<QuerySolution> cypherQueryResult = cypherQueryRunner.runCypherQueryForSolutions(databaseService, EXPERIMENT_QUERY_PREFIX + EXPERIMENT_QUERY_SUFFIX);
        List<QuerySolution> cypherQueryResult = cypherQueryRunner.runCypherQueryForSolutions(databaseService, queryGraph);
        Set<String> uniqueCypherResults = new HashSet<String>();
        for(QuerySolution solution : cypherQueryResult) {
//            System.out.println(solution);
            uniqueCypherResults.add(solution.toString());
        }
        System.out.println("Cypher solution count: " + cypherQueryResult.size());
        System.out.println("Number of unique rows: " + uniqueCypherResults.size());

//        validateQuerySolutions(databaseService, uniqueCypherResults);
//
//
//
//
        compareQuerySolutions(cypherQueryResult, results);

        /* Tear down database */
        databaseService.shutdown();
        System.out.println("foo");
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
