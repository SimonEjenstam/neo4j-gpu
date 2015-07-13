package se.simonevertsson.experiments;

import org.neo4j.graphdb.Relationship;
import se.simonevertsson.db.DatabaseService;

import java.io.IOException;

/**
 * Created by simon on 2015-05-12.
 */
public class Neo4jExperiments {

    public static final String DB_PATH = "target/drwho";
    public static final String DB_CONFIG_PATH = "target";

    public static void main(String[] args) throws IOException {
        DatabaseService dbService = new DatabaseService(DB_PATH, DB_CONFIG_PATH);

        Iterable<Relationship> allRelationships = dbService.getAllRelationships();
        for(Relationship relationship : allRelationships)  {
            System.out.println(relationship.getType());
        }

    }

}
