package se.simonevertsson;

import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.experiments.PerformanceExperiment;

import java.io.IOException;

/**
 * Executes a performance experiment and stores the result in "/results"
 */
public class Main {

  private static String databasePath;
  private static String databaseConfigPath;
  private static int maxQueryGraphNodeCount;
  private static int minQueryGraphRelationshipCount;
  private static int experimentIterations;

  public static void main(String[] args) throws IOException {
    validateInputArguments(args);

    // Setup database connection
    DatabaseService databaseService = new DatabaseService(databasePath, databaseConfigPath);

    PerformanceExperiment performanceExperiment = new PerformanceExperiment(databaseService, maxQueryGraphNodeCount, minQueryGraphRelationshipCount, experimentIterations);
    performanceExperiment.runExperiment();

    //Tear down database
    System.out.println("Terminating database connection...");
    databaseService.shutdown();
    System.out.println("Database connection terminated.");
  }

  private static void validateInputArguments(String[] args) {
    try {
      databasePath = args[0];
      databaseConfigPath = args[1];
      maxQueryGraphNodeCount = Integer.parseInt(args[2]);
      minQueryGraphRelationshipCount = Integer.parseInt(args[3]);
      experimentIterations = Integer.parseInt(args[4]);
    } catch (Exception e) {
      StringBuilder builder = new StringBuilder();
      builder.append("Invalid input arguments.\n");
      builder.append("expected: databasePath(String) databaseConfigPath(String)  maxQueryGraphNodeCount(int) minQueryGraphRelationshipCount(int) experimentIterations(int)\n");
      builder.append("actual:" + String.join(" ", args) + "\n");
      throw new IllegalArgumentException(builder.toString());
    }
  }

}
