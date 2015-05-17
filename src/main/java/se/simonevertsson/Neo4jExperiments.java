package se.simonevertsson;

import com.nativelibs4java.opencl.*;
import org.bridj.Pointer;
import org.neo4j.cypher.internal.compiler.v1_9.symbols.RelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import se.simonevertsson.mocking.GraphMock;
import se.simonevertsson.mocking.Properties;
import se.simonevertsson.mocking.Property;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;

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

//    A1 ----> B2
//    |      / |
//    |    /   |
//    V  y     V
//    A3 ----> C4
    private static QueryGraph generateMockQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        Properties props = Properties.properties(Property.property("foo", "bar"));
        Node A1 = GraphMock.node(1, props, 2, "A");
        Node B2 = GraphMock.node(2, props, 2, "B");
        Node A3 = GraphMock.node(3, props, 1, "A");
        Node C4 = GraphMock.node(4, props, 0, "C");

        queryGraph.nodes.add(A1);
        queryGraph.nodes.add(B2);
        queryGraph.nodes.add(A3);
        queryGraph.nodes.add(C4);

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A1_B2 = GraphMock.relationship(1, props, A1, "test", B2);
        Relationship A1_A3 = GraphMock.relationship(2, props, A1, "test", A3);
        Relationship B2_A3 = GraphMock.relationship(3, props, B2, "test", A3);
        Relationship B2_C4 = GraphMock.relationship(4, props, B2, "test", C4);
        Relationship A3_C4 = GraphMock.relationship(5, props, A3, "test", C4);

        queryGraph.relationships.add(A1_B2);
        queryGraph.relationships.add(A1_A3);
        queryGraph.relationships.add(B2_A3);
        queryGraph.relationships.add(B2_C4);
        queryGraph.relationships.add(A3_C4);

        return queryGraph;
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
