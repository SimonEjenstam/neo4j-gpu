package se.simonevertsson;

import org.neo4j.graphdb.*;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.experiments.*;
import se.simonevertsson.gpu.QueryResult;
import se.simonevertsson.gpu.QuerySolution;
import se.simonevertsson.query.QueryGraph;

import java.io.*;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by simon on 2015-05-12.
 */
public class Main {

    public static final String TEST_DB_PATH = "target/foo";
//    public static final String DB_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j\\cineasts_12k_movies_50k_actors";
    public static final String DB_PATH_PREFIX = "C:\\Users\\simon\\Documents\\Neo4j\\";
//    public static final String DB_PATH = "C:\\Users\\simon.evertsson\\Documents\\Neo4j\\drwho";
    public static final String DR_WHO_DB_CONFIG_PATH = "C:\\Users\\simon\\Documents\\Neo4j";

    public static final String RESULT_OUTPUT_PATH  = "/results";


    public static void main(String[] args) throws IOException {

        String database = args[0];
        int nodeCount = Integer.parseInt(args[1]);
        int maxDepth = Integer.parseInt(args[2]);
        int iterations = Integer.parseInt(args[3]);

        /* Setup database */
        DatabaseService databaseService = new DatabaseService(DB_PATH_PREFIX + database, DR_WHO_DB_CONFIG_PATH);





//        ArrayList<Node> allNodes = databaseService.getAllNodes();
//        ExperimentQueryGraphGenerator experimentQueryGraphGenerator = new ExperimentQueryGraphGenerator(allNodes, 5, 2);
//        QueryGraph queryGraph = experimentQueryGraphGenerator.generate();



        try {
            Date now = new Date(Calendar.getInstance().getTimeInMillis());
            SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HHmm");
//        SimpleDateFormat sdfTime = new SimpleDateFormat("HH:mm");
//            PrintWriter writer = new PrintWriter("results/result-" + sdfDate.format(now) + ".txt", "UTF-8");
            //Whatever the file path is.
            String filename = "results/result-" + sdfDate.format(now) + ".txt";
            System.out.println("Creating result file: " + filename);
            File resultFile = new File(filename);
            FileOutputStream is = new FileOutputStream(resultFile);
            OutputStreamWriter osw = new OutputStreamWriter(is);
            Writer writer = new BufferedWriter(osw);

//            StringBuffer buffer = new StringBuffer();
            writer.append("Result of query on " + database + " with node count " + nodeCount + ", max depth " + maxDepth + " with " + iterations + " iterations\n" );
            writer.append("node_count,gpu_conversion_time,gpu_runtime,cypher_runtime\n");
            writer.append("START\n");

            HashMap<Integer, ArrayList<Long>> gpuConversionTimes = new HashMap<>();
            HashMap<Integer, ArrayList<Long>> gpuRunTimes = new HashMap<>();
            HashMap<Integer, ArrayList<Long>> cypherRunTimes = new HashMap<>();

            for(int i = 2; i <= nodeCount; i++) {
                gpuConversionTimes.put(i, new ArrayList<Long>());
                gpuRunTimes.put(i, new ArrayList<Long>());
                cypherRunTimes.put(i, new ArrayList<Long>());
            }

            ArrayList<QueryGraph> failedQueries = new ArrayList<>();

            for(int i = 0; i < iterations; i++) {

                ArrayList<Node> allNodes = databaseService.getAllNodes();
                MultipleExperimentQueryGraphGenerator experimentQueryGraphGenerator = new MultipleExperimentQueryGraphGenerator(allNodes, nodeCount, 1, maxDepth);
                List<QueryGraph> queryGraphs = experimentQueryGraphGenerator.generate();

                //        RandomQueryGraphGenerator queryGraphGenerator = new RandomQueryGraphGenerator(databaseService);
                //        QueryGraph queryGraph = queryGraphGenerator.generate(4, 2);


                System.out.println("######### ITERATION " + (i + 1) + " ########");
                for (QueryGraph queryGraph : queryGraphs) {
                    if(!failedQueries.contains(queryGraph)) {
                        System.out.println("*********** NEW QUERY  **********");
                        System.out.println("Querying with query:");
                        System.out.println(queryGraph.toCypherQueryString());

                         /* GPU query run */
                        GpuQueryRunner gpuQueryRunner = new GpuQueryRunner();
                        CypherQueryRunner cypherQueryRunner = new CypherQueryRunner();
                        QueryResult gpuQueryResult = null;
                        QueryResult cypherQueryResult = null;
                        try {
                            gpuQueryResult = gpuQueryRunner.runGpuQuery(databaseService, queryGraph);
                            System.out.println("GPU Data conversion execution time: " + gpuQueryResult.getConversionExecutionTime() + "ms");
                            System.out.println("GPU query execution time: " + gpuQueryResult.getQueryExecutionTime() + "ms");

                            //                        Set<String> uniqueResults = new HashSet<String>();
                            //                        for (QuerySolution solution : gpuQueryResult.getQuerySolutions()) {
                            //                            uniqueResults.add(solution.toString());
                            //                        }
                            //                        System.out.println("Number of solutions: " + gpuQueryResult.getQuerySolutions().size());
                            //                        System.out.println("Unique results count: " + uniqueResults.size());


                             /* Cypher query run */

                            System.out.println("Starting Cypher query.");
                            cypherQueryResult = cypherQueryRunner.runCypherQueryForSolutions(databaseService, queryGraph);
                            System.out.println("Cypher query execution time: " + cypherQueryResult.getQueryExecutionTime() + "ms");
                            //
                            //                        Set<String> uniqueCypherResults = new HashSet<String>();
                            //                        for (QuerySolution solution : cypherQueryResult.getQuerySolutions()) {
                            //                            uniqueCypherResults.add(solution.toString());
                            //                        }
                            //                        System.out.println("Cypher solution count: " + cypherQueryResult.getQuerySolutions().size());
                            //                        System.out.println("Number of unique rows: " + uniqueCypherResults.size());
                            //                        System.out.println("Speedup: " + ((double) cypherQueryResult.getTotalExecutionTime() / (double) gpuQueryResult.getTotalExecutionTime()) + "x");

                        } catch (OutOfMemoryError e) {
                            System.out.println("QUERY FAILED!!!");
                            failedQueries.add(queryGraph);
                            continue;
                        }

                        //        /* Validate solutions */
                        //        validateQuerySolutions(databaseService, uniqueResults);

                        System.out.println("Speedup excluding conversion time: " + ((double) cypherQueryResult.getTotalExecutionTime() / (double) gpuQueryResult.getQueryExecutionTime()) + "x");

                        gpuConversionTimes.get(queryGraph.nodes.size()).add(gpuQueryResult.getConversionExecutionTime());
                        gpuRunTimes.get(queryGraph.nodes.size()).add(gpuQueryResult.getQueryExecutionTime());
                        cypherRunTimes.get(queryGraph.nodes.size()).add(cypherQueryResult.getQueryExecutionTime());

                        writer.append("" + queryGraph.nodes.size());
                        writer.append(",");
                        writer.append("" + gpuQueryResult.getConversionExecutionTime());
                        writer.append(",");
                        writer.append("" + gpuQueryResult.getQueryExecutionTime());
                        writer.append(",");
                        writer.append("" + cypherQueryResult.getQueryExecutionTime());
                        writer.append("\n");

                        //        validateQuerySolutions(databaseService, uniqueCypherResults);
                        //
                        //
                        //
                        //
                        //        compareQuerySolutions(cypherQueryResult, results);
                    }

                }
            }

            writer.append("END\n");

            for(int i = 2; i <= nodeCount; i++) {
                double conversionAvg = calculateMean(gpuConversionTimes.get(i));
                double gpuAvg = calculateMean(gpuRunTimes.get(i));
                double cypherAvg = calculateMean(cypherRunTimes.get(i));
                writer.append("-----Averages for query with " + i + " nodes ----\n");
                writer.append("Average GPU conversion time: " + conversionAvg + " ms\n");
                writer.append("Average GPU run time: " + gpuAvg + " ms\n");
                writer.append("Average Cypher run time: " + cypherAvg + " ms\n");

            }
            writer.append("----Failed queries----\n");
            for(QueryGraph failedQuery : failedQueries) {
                writer.append(failedQuery.toCypherQueryString()+"\n");
            }
//        writer.append(buffer.toString());
            writer.close();



        } catch (IOException e) {
            System.err.println("Problem writing to the file statsTest.txt");
        }



        /* Tear down database */
        System.out.println("Terminating database connection...");
        databaseService.shutdown();
        System.out.println("Database connection terminated.");
    }

    private static double calculateMean(ArrayList<Long> values) {
        long sum = 0;
        for(long value : values) {
            sum += value;
        }
        return (double)sum / (double) values.size();
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
