package se.simonevertsson.experiments;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.gpu.QueryKernels;
import se.simonevertsson.gpu.QueryResult;
import se.simonevertsson.gpu.QuerySolution;
import se.simonevertsson.query.QueryGraph;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class Experiment {

  private final DatabaseService databaseService;
  private final int maxQueryGraphNodeCount;
  private final int minQueryGraphRelationshipCount;
  private final int experimentIterations;

  public Experiment(DatabaseService databaseService, int maxQueryGraphNodeCount, int minQueryGraphRelationshipCount, int experimentIterations) {
    this.databaseService = databaseService;
    this.maxQueryGraphNodeCount = maxQueryGraphNodeCount;
    this.minQueryGraphRelationshipCount = minQueryGraphRelationshipCount;
    this.experimentIterations = experimentIterations;
  }

  public void runExperiment() {
    try {
      Writer writer = initializeFileWriter();

      HashMap<Integer, ArrayList<Long>> gpuConversionTimes = new HashMap<>();
      HashMap<Integer, ArrayList<Long>> gpuRunTimes = new HashMap<>();
      HashMap<Integer, ArrayList<Long>> cypherRunTimes = new HashMap<>();

      for (int i = 2; i <= maxQueryGraphNodeCount; i++) {
        gpuConversionTimes.put(i, new ArrayList<Long>());
        gpuRunTimes.put(i, new ArrayList<Long>());
        cypherRunTimes.put(i, new ArrayList<Long>());
      }

      ArrayList<QueryGraph> failedQueries = new ArrayList<>();

      ArrayList<QueryGraph> slowQueries = new ArrayList<>();

      List<Node> allNodes = databaseService.getAllNodes();
      SingleExperimentQueryGraphGenerator experimentQueryGraphGenerator = new SingleExperimentQueryGraphGenerator(allNodes, maxQueryGraphNodeCount, 1, minQueryGraphRelationshipCount);
      ArrayList<QueryGraph> queryGraphs = experimentQueryGraphGenerator.generate(experimentIterations);
      System.out.println("Generated " + queryGraphs.size() + " query graphs");
      QueryKernels queryKernels = new QueryKernels();

      for (int i = 0; i < 10; i++) {
        Result result = databaseService.excuteCypherQuery("match (n)-[r]-() return count(n)");
        result.close();
      }

//            for(int i = 1; i < iterations; i++) {
//                    allNodes = databaseService.getAllNodes();
//                    experimentQueryGraphGenerator = new MultipleExperimentQueryGraphGenerator(allNodes, nodeCount, 1, minRelationships);
//                    queryGraphIterations.add(experimentQueryGraphGenerator.generate(queryGraphIterations.get(i-1).get(0)));
//            }

//            for(int i = 0; i < iterations; i++) {

      //        RandomQueryGraphGenerator queryGraphGenerator = new RandomQueryGraphGenerator(databaseService);
      //        QueryGraph queryGraph = queryGraphGenerator.generate(4, 2);
//                ArrayList<QueryGraph> queryGraphs = queryGraphIterations.get(i);

      int iter = 0;
      for (QueryGraph queryGraph : queryGraphs) {
        if (iter % (maxQueryGraphNodeCount - 1) == 0) {
          System.out.println("######### ITERATION " + (iter + 1) + " ########");
        }
        if (!failedQueries.contains(queryGraph)) {
          System.out.println("*********** NEW QUERY  **********");
          System.out.println("Querying with query:");
          System.out.println(queryGraph.toCypherQueryString());

                         /* GPU query run */
          GpuQueryRunner gpuQueryRunner = new GpuQueryRunner();
          CypherQueryRunner cypherQueryRunner = new CypherQueryRunner();
          QueryResult gpuQueryResult = null;
          QueryResult cypherQueryResult = null;
          try {

            gpuQueryResult = gpuQueryRunner.runGpuQuery(databaseService, queryGraph, queryKernels);
            System.out.println("GPU Data conversion execution time: " + gpuQueryResult.getConversionExecutionTime() + "ms");
            System.out.println("GPU query execution time: " + gpuQueryResult.getQueryExecutionTime() + "ms");

            //                        Set<String> uniqueResults = new HashSet<String>();
            //                        for (QuerySolution solution : gpuQueryResult.getQuerySolutions()) {
            //                            uniqueResults.add(solution.toString());
            //                        }
            System.out.println("Number of solutions: " + gpuQueryResult.getQuerySolutions().size());
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
            System.out.println("Cypher solution count: " + cypherQueryResult.getQuerySolutions().size());
            //                        System.out.println("Number of unique rows: " + uniqueCypherResults.size());
            //                        System.out.println("Speedup: " + ((double) cypherQueryResult.getTotalExecutionTime() / (double) gpuQueryResult.getTotalExecutionTime()) + "x");


            System.out.println("Speedup excluding conversion time: " + ((double) cypherQueryResult.getTotalExecutionTime() / (double) gpuQueryResult.getQueryExecutionTime()) + "x");

            if (gpuQueryResult.getQueryExecutionTime() > cypherQueryResult.getQueryExecutionTime()) {
              slowQueries.add(queryGraph);
            }

            if (iter > 1) {
              gpuConversionTimes.get(queryGraph.nodes.size()).add(gpuQueryResult.getConversionExecutionTime());
              gpuRunTimes.get(queryGraph.nodes.size()).add(gpuQueryResult.getQueryExecutionTime());
              cypherRunTimes.get(queryGraph.nodes.size()).add(cypherQueryResult.getQueryExecutionTime());

              writer.append("" + queryGraph.nodes.size());
              writer.append(",");
              writer.append("" + gpuQueryResult.getQuerySolutions().size());
              writer.append(",");
              writer.append("" + gpuQueryResult.getConversionExecutionTime());
              writer.append(",");
              writer.append("" + gpuQueryResult.getQueryExecutionTime());
              writer.append(",");
              writer.append("" + cypherQueryResult.getQueryExecutionTime());
              writer.append("\n");
            }


          } catch (OutOfMemoryError | IllegalArgumentException e) {
            System.out.println("QUERY FAILED!!!");
            System.out.println(e.getMessage());
            for (StackTraceElement element : e.getStackTrace()) {
              System.out.println(element);
            }
            failedQueries.add(queryGraph);
            continue;
          }

          //        /* Validate solutions */
          //        validateQuerySolutions(databaseService, uniqueResults);


          //        validateQuerySolutions(databaseService, uniqueCypherResults);
          //
          //
          //
          //
          //        compareQuerySolutions(cypherQueryResult, results);
        }
        iter++;
      }
//            }

      writer.append("END\n");

      for (int i = 2; i <= maxQueryGraphNodeCount; i++) {
        double conversionRunTimeAvg = calculateMean(gpuConversionTimes.get(i));
        double conversionRunTimeStdDev = calculateStandardDeviation(gpuConversionTimes.get(i), conversionRunTimeAvg);
        double conversionSpeedupAvg = calculateConversionSpeedupMean(gpuConversionTimes.get(i), gpuRunTimes.get(i), cypherRunTimes.get(i));
        double conversionSpeedupStdDev = calculateConversionSpeedupStandardDeviation(gpuConversionTimes.get(i), gpuRunTimes.get(i), cypherRunTimes.get(i), conversionSpeedupAvg);

        double gpuRuntimeAvg = calculateMean(gpuRunTimes.get(i));
        double gpuRuntimeStdDev = calculateStandardDeviation(gpuRunTimes.get(i), gpuRuntimeAvg);
        double gpuSpeedupAvg = calculateGpuSpeedupMean(gpuRunTimes.get(i), cypherRunTimes.get(i));
        double gpuSpeedupStdDev = calculateGpuSpeedupStandardDeviation(gpuRunTimes.get(i), cypherRunTimes.get(i), gpuSpeedupAvg);

        double cypherAvg = calculateMean(cypherRunTimes.get(i));
        double cypherStdDev = calculateStandardDeviation(cypherRunTimes.get(i), cypherAvg);


        writer.append("-----Averages for query with " + i + " nodes ----\n");
        writer.append("Average GPU conversion time: " + conversionRunTimeAvg + " ms , stdDev: " + conversionRunTimeStdDev + "ms,  avg. speedup: (" + i + ", " + conversionSpeedupAvg + ")  speedup stdDev: (" + conversionSpeedupStdDev + ")\n");
        writer.append("Average GPU run time: " + gpuRuntimeAvg + " ms , stdDev: " + gpuRuntimeStdDev + "ms,  avg. speedup: (" + i + ", " + gpuSpeedupAvg + ") speedup stdDev: (" + gpuSpeedupStdDev + ")\n");
        writer.append("Average Cypher run time: " + cypherAvg + " ms, stdDev: " + cypherStdDev + "ms\n");

      }
      writer.append("----Failed queries----\n");
      for (QueryGraph failedQuery : failedQueries) {
        writer.append(failedQuery.toCypherQueryString() + "\n");
      }

      writer.append("----Slow queries----\n");
      for (QueryGraph slowQuery : slowQueries) {
        writer.append(slowQuery.toCypherQueryString() + "\n");
        writer.append("---------------------");
      }


//        writer.append(buffer.toString());
      writer.close();


    } catch (IOException e) {
      System.err.println("Problem writing to the file statsTest.txt");
    }
  }

  private Writer initializeFileWriter() throws IOException {
    Date now = new Date(Calendar.getInstance().getTimeInMillis());
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HHmm");

    String databaseName = databaseService.getDatabaseName();

    // Whatever the file path is.
    String filename = "results/result-" + databaseName + '-' + maxQueryGraphNodeCount + '-' + minQueryGraphRelationshipCount + '-' + sdfDate.format(now) + ".txt";
    System.out.println("Creating result file: " + filename);
    File resultFile = new File(filename);
    FileOutputStream is = new FileOutputStream(resultFile);
    OutputStreamWriter osw = new OutputStreamWriter(is);
    Writer writer = new BufferedWriter(osw);

    writer.append("Result of query on " + databaseName + " with node count " + maxQueryGraphNodeCount + ", min relationships " + minQueryGraphRelationshipCount + " with " + experimentIterations + " iterations\n");
    writer.append("node_count, result_count, gpu_conversion_time,gpu_runtime,cypher_runtime\n");
    writer.append("START\n");
    return writer;
  }

  private static double calculateGpuSpeedupStandardDeviation(ArrayList<Long> gpuRuntimes, ArrayList<Long> cypherRuntimes, double gpuSpeedupAvg) {
    double deviationSum = 0;
    for (int i = 0; i < gpuRuntimes.size(); i++) {
      double dataPoint = (double) cypherRuntimes.get(i) / (double) gpuRuntimes.get(i);
      deviationSum += Math.pow(dataPoint - gpuSpeedupAvg, 2);
    }
    return Math.sqrt(deviationSum / (double) gpuRuntimes.size());
  }

  private static double calculateGpuSpeedupMean(ArrayList<Long> gpuRuntimes, ArrayList<Long> cypherRuntimes) {
    double sum = 0;
    for (int i = 0; i < gpuRuntimes.size(); i++) {
      sum += (double) cypherRuntimes.get(i) / (double) gpuRuntimes.get(i);
    }
    return sum / (double) gpuRuntimes.size();
  }

  private static double calculateConversionSpeedupStandardDeviation(ArrayList<Long> conversionRuntimes, ArrayList<Long> gpuRuntimes, ArrayList<Long> cypherRuntimes, double conversionSpeedupAvg) {
    double deviationSum = 0;
    for (int i = 0; i < conversionRuntimes.size(); i++) {
      double dataPoint = (double) cypherRuntimes.get(i) / (double) (conversionRuntimes.get(i) + gpuRuntimes.get(i));
      deviationSum += Math.pow(dataPoint - conversionSpeedupAvg, 2);
    }
    return Math.sqrt(deviationSum / (double) conversionRuntimes.size());
  }

  private static double calculateConversionSpeedupMean(ArrayList<Long> conversionRuntimes, ArrayList<Long> gpuRuntimes, ArrayList<Long> cypherRuntimes) {
    double sum = 0;
    for (int i = 0; i < conversionRuntimes.size(); i++) {
      sum += (double) cypherRuntimes.get(i) / (double) (conversionRuntimes.get(i) + gpuRuntimes.get(i));
    }
    return sum / (double) conversionRuntimes.size();
  }

  private static double calculateStandardDeviation(ArrayList<Long> conversionRunTimes, double conversionAvg) {
    double deviationSum = 0;
    for (long conversionRunTime : conversionRunTimes) {
      deviationSum += Math.pow((double) conversionRunTime - conversionAvg, 2);
    }
    return Math.sqrt(deviationSum / (double) conversionRunTimes.size());
  }

  private static double calculateMean(ArrayList<Long> values) {
    long sum = 0;
    for (long value : values) {
      sum += value;
    }
    return (double) sum / (double) values.size();
  }

  private static void compareQuerySolutions(List<QuerySolution> cypherQueryResult, List<QuerySolution> gpuQueryResult) {
    List<QuerySolution> cypherQueryResultClone = new ArrayList<>(cypherQueryResult);
    for (QuerySolution gpuSolution : gpuQueryResult) {
      Iterator<QuerySolution> itr = cypherQueryResultClone.iterator();
      while (itr.hasNext()) {
        QuerySolution cypherSolution = itr.next();
        if (gpuSolution.equals(cypherSolution)) {
          itr.remove();
          break;
        }
      }
    }

    for (QuerySolution missingGpuSolution : cypherQueryResultClone) {
      System.out.println("GPU result does not contain Cypher solution below");
      System.out.println(missingGpuSolution);
    }


    List<QuerySolution> gpuQueryResultClone = new ArrayList<>(gpuQueryResult);
    for (QuerySolution cypherSolution : cypherQueryResult) {
      Iterator<QuerySolution> itr = gpuQueryResultClone.iterator();
      while (itr.hasNext()) {
        QuerySolution gpuSolution = itr.next();
        if (cypherSolution.equals(gpuSolution)) {
          itr.remove();
          break;
        }
      }

    }

    for (QuerySolution missingCypherSolution : gpuQueryResultClone) {
      System.out.println("Cypher result does not contain GPU solution below");
      System.out.println(missingCypherSolution);
    }

    System.out.println("Missing Cypher solution count: " + cypherQueryResultClone.size());
    System.out.println("Missing GPU solution count: " + gpuQueryResultClone.size());
  }

  private static void validateQuerySolutions(DatabaseService databaseService, Set<String> results) {
    try (Transaction tx = databaseService.beginTx()) {
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
