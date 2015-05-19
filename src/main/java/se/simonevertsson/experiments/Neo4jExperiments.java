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
//        Iterable<Node> allNodes = dbService.getAllNodes();
//        for(Node node : allNodes)  {
//            System.out.println(node.toString() + " labels:");
//            for (Label label : node.getLabels()) {
//                System.out.println(label.name());
//            }
//        }

        Iterable<Relationship> allRelationships = dbService.getAllRelationships();
        for(Relationship relationship : allRelationships)  {
            System.out.println(relationship.getType());
        }

    }

//    Doctor ----> Master

//    private static QueryGraph generateMockQueryGraph() {
//
//        QueryGraph queryGraph = new QueryGraph();
//
//        queryGraph.nodes = new ArrayList<Node>();
//
//        Properties props = Properties.properties(Property.property("foo", "bar"));
//        Node doctor = GraphMock.node(1, props, 1, "Doctor");
//        Node master = GraphMock.node(2, props, 0, "Master");
//
//        queryGraph.nodes.add(doctor);
//        queryGraph.nodes.add(master);
//
//        queryGraph.relationships = new ArrayList<Relationship>();
//
//        Relationship A1_B2 = GraphMock.relationship(1, props, doctor, "test", B2);
//
//        queryGraph.relationships.add(A1_B2);
//        queryGraph.relationships.add(A1_A3);
//        queryGraph.relationships.add(B2_A3);
//        queryGraph.relationships.add(B2_C4);
//        queryGraph.relationships.add(A3_C4);
//
//        return queryGraph;
//    }

}
