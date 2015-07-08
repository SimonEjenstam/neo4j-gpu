package se.simonevertsson.db;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;
import se.simonevertsson.experiments.RelationshipTypes;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by simon on 2015-05-12.
 */
public class DatabaseService {

    private GlobalGraphOperations graphOperations;
    private GraphDatabaseService graphDatabase;

    public DatabaseService(String databasePath, String configPath) {
        this.graphDatabase = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(databasePath)
                .loadPropertiesFromFile(configPath + "/neo4j.properties")
                .newGraphDatabase();
        this.graphOperations = GlobalGraphOperations.at(this.graphDatabase);
        registerShutdownHook(this.graphDatabase);
    }

    public DatabaseService(String dbPath) throws IOException {
        createDb(dbPath);
        this.graphOperations = GlobalGraphOperations.at(this.graphDatabase);
    }

    private void registerShutdownHook( final GraphDatabaseService graphDb )
    {
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


    void createDb(String dbPath) throws IOException
    {
        FileUtils.deleteRecursively(new File(dbPath));
        this.graphDatabase = new GraphDatabaseFactory().newEmbeddedDatabase( dbPath );
        registerShutdownHook(this.graphDatabase);
    }

    public ResourceIterable<Node> getAllNodes() {
        ResourceIterable<Node> result = null;
        try {
            Transaction tx = this.graphDatabase.beginTx();
            result = this.graphOperations.getAllNodes();
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Iterable<Relationship> getAllRelationships() {
        Iterable<Relationship> result = null;
        try {
            Transaction tx = this.graphDatabase.beginTx();
            result = this.graphOperations.getAllRelationships();
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public void deleteData() {
        try {
            Transaction tx = this.graphDatabase.beginTx();
            deleteAllRelationships();
            deleteAllNodes();
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void deleteAllRelationships() {
        Iterable<Relationship> allRelationships = this.graphOperations.getAllRelationships();
        for(Relationship relationship : allRelationships) {
            relationship.delete();
        }
    }

    private void deleteAllNodes() {
        Iterable<Node> allNodes = this.graphOperations.getAllNodes();
        for(Node node : allNodes) {
            node.delete();
        }
    }

    public GraphDatabaseService getGraphDatabase() {
        return this.graphDatabase;
    }

    public Result excuteCypherQueryWithinTransaction(String query) {
        Transaction transaction = this.graphDatabase.beginTx();
        Result result = this.graphDatabase.execute(
                query);
        transaction.success();
        return result;
    }

    public Result excuteCypherQuery(String query) {
        Result result = this.graphDatabase.execute(
                query);
        return result;
    }

    public void shutdown() {
        this.graphDatabase.shutdown();
    }

    public Transaction beginTx() {
        return this.graphDatabase.beginTx();
    }
}
