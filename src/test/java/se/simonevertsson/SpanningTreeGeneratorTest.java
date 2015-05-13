package se.simonevertsson;

import junit.framework.TestCase;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.mocking.GraphMock;
import se.simonevertsson.mocking.Properties;
import se.simonevertsson.mocking.Property;

import java.util.ArrayList;

/**
 * Created by simon on 2015-05-12.
 */
public class SpanningTreeGeneratorTest extends TestCase {

    public void testGenerateQueryGraph() throws Exception {
        // Given
        QueryGraph queryGraph = generateMockQueryGraph();

        // When
        QueryGraph result = SpanningTreeGenerator.generateQueryGraph(queryGraph);

        // Then
        assertEquals(3, result.spanningTree.size());
        assertEquals(1, result.visitOrder.size());
        assertEquals(2, (long)result.visitOrder.get(0));
    }

//    A1 ----> B2
//    |      / |
//    |    /   |
//    V  y     V
//    A3 ----> C4
    private QueryGraph generateMockQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        Properties props = Properties.properties(Property.property("foo", "bar"));
        Node A1 = GraphMock.node(1, props, 2, "A");
        Node B2 = GraphMock.node(2, props, 3, "B");
        Node A3 = GraphMock.node(3, props, 3, "A");
        Node C4 = GraphMock.node(4, props, 2, "C");

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
}