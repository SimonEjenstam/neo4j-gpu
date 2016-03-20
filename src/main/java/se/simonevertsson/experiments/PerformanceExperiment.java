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

import static se.simonevertsson.experiments.StatisticsUtils.*;

/**
 * Class which is used to execute a experiment which compare the query performance of the GPU queries vs. regular Cypher queries
 * The result of the experiment is written to a file in "results"-directory relative to the execution root.
 */
public class PerformanceExperiment {

  private final DatabaseService databaseService;
  private final int maxQueryGraphNodeCount;
  private final int minQueryGraphRelationshipCount;
  private final int experimentIterations;
  private HashMap<Integer, ArrayList<Long>> gpuConversionTimes;
  private HashMap<Integer, ArrayList<Long>> gpuRunTimes;
  private HashMap<Integer, ArrayList<Long>> cypherRunTimes;
  private Writer writer;
  private ArrayList<QueryGraph> failedQueries;
  private ArrayList<QueryGraph> slowQueries;
  private QueryKernels queryKernels;

  public PerformanceExperiment(DatabaseService databaseService, int maxQueryGraphNodeCount, int minQueryGraphRelationshipCount, int experimentIterations) {
    this.databaseService = databaseService;
    this.maxQueryGraphNodeCount = maxQueryGraphNodeCount;
    this.minQueryGraphRelationshipCount = minQueryGraphRelationshipCount;
    this.experimentIterations = experimentIterations;
  }

  public void runExperiment() {
    try {
      intializeExperiment();
      List<QueryGraph> queryGraphs = generateQueryGraphs();
      warmDatabaseCaches();
      executeQueries(queryGraphs);
      writeStatisticalResultsToFile();
      writer.close();
    } catch (IOException e) {
      System.err.println("Problem writing to the result file");
      e.printStackTrace();
    }
  }

  private void executeQueries(List<QueryGraph> queryGraphs) throws IOException {
    int iteration = 0;
    for (QueryGraph queryGraph : queryGraphs) {
      if (iteration % (maxQueryGraphNodeCount - 1) == 0) {
        System.out.println("######### ITERATION " + (iteration + 1) + " ########");
      }
      if (!failedQueries.contains(queryGraph)) {
        if (!executeGpuAndCypherQuery(iteration, queryGraph)) continue;
      }
      iteration++;
    }
  }

  private boolean executeGpuAndCypherQuery(int iteration, QueryGraph queryGraph) throws IOException {
    System.out.println("*********** NEW QUERY  **********");
    System.out.println("Querying with query:");
    System.out.println(queryGraph.toCypherQueryString());

    GpuQueryRunner gpuQueryRunner = new GpuQueryRunner();
    CypherQueryRunner cypherQueryRunner = new CypherQueryRunner();
    QueryResult gpuQueryResult = null;
    QueryResult cypherQueryResult = null;
    try {
      gpuQueryResult = executeGpuQuery(queryGraph, gpuQueryRunner);
      cypherQueryResult = executeCyperQuery(queryGraph, cypherQueryRunner);
      System.out.println("Speedup excluding conversion time: " + ((double) cypherQueryResult.getTotalExecutionTime() / (double) gpuQueryResult.getQueryExecutionTime()) + "x");

      // We log slow queries since they are targets for analysis
      if (gpuQueryResult.getQueryExecutionTime() > cypherQueryResult.getQueryExecutionTime()) {
        slowQueries.add(queryGraph);
      }

      // Ignore first iteration since database caches may still be cold (poor Cypher performance -> skewed speedup values)
      if (iteration > 1) {
        gpuConversionTimes.get(queryGraph.nodes.size()).add(gpuQueryResult.getConversionExecutionTime());
        gpuRunTimes.get(queryGraph.nodes.size()).add(gpuQueryResult.getQueryExecutionTime());
        cypherRunTimes.get(queryGraph.nodes.size()).add(cypherQueryResult.getQueryExecutionTime());
        writeIterationResultToFile(queryGraph, gpuQueryResult, cypherQueryResult);
      }
    } catch (OutOfMemoryError | IllegalArgumentException e) {
      // TODO Handle memory issues more gracefully.
      System.out.println("QUERY FAILED!!!");
      System.out.println(e.getMessage());
      for (StackTraceElement element : e.getStackTrace()) {
        System.out.println(element);
      }

      // We log failing queries since they are targets for analysis
      failedQueries.add(queryGraph);
      return false;
    }
    return true;
  }

  private QueryResult executeCyperQuery(QueryGraph queryGraph, CypherQueryRunner cypherQueryRunner) {
    QueryResult cypherQueryResult;
    System.out.println("Starting Cypher query.");
    cypherQueryResult = cypherQueryRunner.runCypherQueryForSolutions(databaseService, queryGraph);
    System.out.println("Cypher query execution time: " + cypherQueryResult.getQueryExecutionTime() + "ms");
    System.out.println("Cypher solution count: " + cypherQueryResult.getQuerySolutions().size());
    return cypherQueryResult;
  }

  private QueryResult executeGpuQuery(QueryGraph queryGraph, GpuQueryRunner gpuQueryRunner) throws IOException {
    QueryResult gpuQueryResult;
    System.out.println("Starting GPU query.");
    gpuQueryResult = gpuQueryRunner.runGpuQuery(databaseService, queryGraph, queryKernels);
    System.out.println("GPU Data conversion execution time: " + gpuQueryResult.getConversionExecutionTime() + "ms");
    System.out.println("GPU query execution time: " + gpuQueryResult.getQueryExecutionTime() + "ms");
    System.out.println("Number of solutions: " + gpuQueryResult.getQuerySolutions().size());
    return gpuQueryResult;
  }

  private void writeIterationResultToFile(QueryGraph queryGraph, QueryResult gpuQueryResult, QueryResult cypherQueryResult) throws IOException {
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

  private void writeStatisticalResultsToFile() throws IOException {
    writer.append("EXPERIMENT FINISHED\n");

    for (int i = 2; i <= maxQueryGraphNodeCount; i++) {
      // Calculate data conversion run time statistics, to get an understanding of how the data conversion influences performance
      double conversionRunTimeAvg = mean(gpuConversionTimes.get(i));
      double conversionRunTimeStdDev = stdDev(gpuConversionTimes.get(i), conversionRunTimeAvg);
      double speedupIncludingConversionAvg =
          meanOfSpeedupIncludingConversionTime(gpuConversionTimes.get(i), gpuRunTimes.get(i), cypherRunTimes.get(i));
      double speedupIncludingConversionStdDev =
          stdDevOfSpeedupIncludingConversionTime(gpuConversionTimes.get(i), gpuRunTimes.get(i), cypherRunTimes.get(i), speedupIncludingConversionAvg);

      // Calculate GPU runtime statistics (excluding the data conversion runt times)
      double gpuRuntimeAvg = mean(gpuRunTimes.get(i));
      double gpuRuntimeStdDev = stdDev(gpuRunTimes.get(i), gpuRuntimeAvg);
      double gpuSpeedupAvg = meanOfGpuSpeedup(gpuRunTimes.get(i), cypherRunTimes.get(i));
      double gpuSpeedupStdDev = stdDevOfGpuSpeedup(gpuRunTimes.get(i), cypherRunTimes.get(i), gpuSpeedupAvg);

      // Calculate Cypher query run time statistics
      double cypherAvg = mean(cypherRunTimes.get(i));
      double cypherStdDev = stdDev(cypherRunTimes.get(i), cypherAvg);

      // Append the values to the result file
      writer.append("-----Statistical results for query with " + i + " nodes ----\n");
      writer.append("Average GPU conversion time: " + conversionRunTimeAvg + " ms , stdDev: " + conversionRunTimeStdDev + "ms,  avg. speedup: (" + i + ", " + speedupIncludingConversionAvg + ")  speedup stdDev: (" + speedupIncludingConversionStdDev + ")\n");
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
  }

  /**
   * Executes a query which touches all nodes and relationships to warm the database caches
   */
  private void warmDatabaseCaches() {
    for (int i = 0; i < 10; i++) {
      Result result = databaseService.excuteCypherQuery("match (n)-[r]-() return count(n)");
      result.close();
    }
  }

  private List<QueryGraph> generateQueryGraphs() {
    List<Node> allNodes = databaseService.getAllNodes();
    System.out.println("Database connection established and all nodes fetched. Press a key to start experiment...");
    new Scanner(System.in).nextLine();

    QueryGraphGenerator experimentQueryGraphGenerator = new QueryGraphGenerator(allNodes, maxQueryGraphNodeCount, minQueryGraphRelationshipCount);
    List<QueryGraph> queryGraphs = experimentQueryGraphGenerator.generate(experimentIterations);
    System.out.println("Generated " + queryGraphs.size() + " query graphs");
    return queryGraphs;
  }

  private void intializeExperiment() throws IOException {
    initializeFileWriter();
    initializeRuntimeMaps();
    failedQueries = new ArrayList<>();
    slowQueries = new ArrayList<>();
    queryKernels = new QueryKernels();
  }

  private void initializeRuntimeMaps() {
    gpuConversionTimes = new HashMap<>();
    gpuRunTimes = new HashMap<>();
    cypherRunTimes = new HashMap<>();

    for (int i = 2; i <= maxQueryGraphNodeCount; i++) {
      gpuConversionTimes.put(i, new ArrayList<Long>());
      gpuRunTimes.put(i, new ArrayList<Long>());
      cypherRunTimes.put(i, new ArrayList<Long>());
    }
  }

  private void initializeFileWriter() throws IOException {
    Date now = new Date(Calendar.getInstance().getTimeInMillis());
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HHmm");

    String databaseName = databaseService.getDatabaseName();

    // Whatever the file path is.
    String filename = "results/result-" + databaseName + '-' + maxQueryGraphNodeCount + '-' + minQueryGraphRelationshipCount + '-' + sdfDate.format(now) + ".txt";
    System.out.println("Creating result file: " + filename);
    File resultFile = new File(filename);
    FileOutputStream is = new FileOutputStream(resultFile);
    OutputStreamWriter osw = new OutputStreamWriter(is);

    writer = new BufferedWriter(osw);
    writer.append("Result of query on " + databaseName + " with node count " + maxQueryGraphNodeCount + ", min relationships " + minQueryGraphRelationshipCount + " with " + experimentIterations + " iterations\n");
    writer.append("node_count, result_count, gpu_conversion_time,gpu_runtime,cypher_runtime\n");
    writer.append("EXPERIMENT STARTED\n");
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

