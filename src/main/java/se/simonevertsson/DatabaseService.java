package se.simonevertsson;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.management.relation.Relation;
import javax.xml.crypto.Data;
import java.util.Iterator;

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

    public DatabaseService(GraphDatabaseService graphDb) {
        this.graphDatabase = graphDb;
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
}
