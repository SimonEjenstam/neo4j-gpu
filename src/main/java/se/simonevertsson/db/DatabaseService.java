package se.simonevertsson.db;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;
import se.simonevertsson.experiments.RelationshipTypes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Service class which handles database connections and queries
 */
public class DatabaseService {

  private final String databasePath;
  private GlobalGraphOperations graphOperations;
  private GraphDatabaseService graphDatabase;

  /**
   * Creates a new connection to a Neo4j database
   *
   * @param databasePath The absolute path to the the database directory.
   * @param configPath   The absolute path to the database configuration file.
   */
  public DatabaseService(String databasePath, String configPath) {
    this.databasePath = databasePath;
    this.graphDatabase = new GraphDatabaseFactory()
        .newEmbeddedDatabaseBuilder(databasePath)
        .loadPropertiesFromFile(configPath)
        .newGraphDatabase();
    this.graphOperations = GlobalGraphOperations.at(this.graphDatabase);
    registerShutdownHook(this.graphDatabase);
  }

  private void registerShutdownHook(final GraphDatabaseService graphDb) {
    // Registers a shutdown hook for the Neo4j instance so that it
    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
    // running application).
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        graphDb.shutdown();
      }
    });
  }

  private boolean isConnected() {
    return graphDatabase != null;
  }

  /**
   * Retrieves all nodes in the currently connected database
   *
   * @return A list of all nodes in the database
   */
  public List<Node> getAllNodes() {
    ResourceIterable<Node> result = null;
    ArrayList<Node> allNodes = new ArrayList<>();
    if (isConnected()) {
      try {
        Transaction tx = this.graphDatabase.beginTx();
        result = this.graphOperations.getAllNodes();
        tx.success();
      } catch (Exception e) {
        e.printStackTrace();
      }
      for (Node node : result) {
        allNodes.add(node);
      }
    }

    return allNodes;
  }

  /**
   * Retrieves all relationships in the currently connected database
   *
   * @return A list of all relationships in the database
   */
  public ArrayList<Relationship> getAllRelationships() {
    Iterable<Relationship> result = null;
    ArrayList<Relationship> allRelationships = new ArrayList<>();
    if (isConnected()) {
      try {
        Transaction tx = this.graphDatabase.beginTx();
        result = this.graphOperations.getAllRelationships();
        tx.success();
      } catch (Exception e) {
        e.printStackTrace();
      }
      for (Relationship relationship : result) {
        allRelationships.add(relationship);
      }
    }

    return allRelationships;
  }

  public Transaction beginTx() {
    return this.graphDatabase.beginTx();
  }

  /**
   * Executes the supplied Cypher query on the currently conected database. Remember to call {@link DatabaseService#beginTx()}
   * before executing queries, or Neo4j will throw an exception.
   *
   * @param query The Cypher query which will be executed.
   * @return The result of the query
   */
  public Result excuteCypherQuery(String query) {
    Result result = null;
    if (isConnected()) {
      result = this.graphDatabase.execute(query);
    }

    return result;
  }

  /**
   * Terminates the current database connection
   */
  public void shutdown() {
    this.graphDatabase.shutdown();
    this.graphDatabase = null;
  }

  public String getDatabaseName() {
    String databaseName = null;
    if(databasePath != null && databasePath.lastIndexOf("/") >= 0) {
      databaseName = databasePath.substring(databasePath.lastIndexOf("/"));
    }

    return databaseName;
  }

}
