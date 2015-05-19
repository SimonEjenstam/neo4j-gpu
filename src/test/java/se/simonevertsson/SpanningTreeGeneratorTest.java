package se.simonevertsson;

import junit.framework.TestCase;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import se.simonevertsson.gpu.LabelDictionary;
import se.simonevertsson.gpu.SpanningTreeGenerator;
import se.simonevertsson.query.QueryGraph;
import se.simonevertsson.query.QueryLabel;
import se.simonevertsson.query.QueryNode;

import java.util.ArrayList;

/**
 * Created by simon on 2015-05-12.
 */
public class SpanningTreeGeneratorTest extends TestCase {

    // START SNIPPET: createReltype
    private static enum RelTypes implements RelationshipType
    {
        KNOWS
    }
    // END SNIPPET: createReltype


    public void testGenerateQueryGraph() throws Exception {
        // Given
        QueryGraph queryGraph = generateMockQueryGraph();
        LabelDictionary labelDictionary = new LabelDictionary();

        // When
        QueryGraph result = SpanningTreeGenerator.generateQueryGraph(queryGraph, labelDictionary);

        // Then
        assertEquals(3, result.spanningTree.size());
        assertEquals(4, result.visitOrder.size());
        assertEquals(2, (long)result.visitOrder.get(0).getId());
        assertEquals(3, (long)result.visitOrder.get(1).getId());
        assertEquals(4, (long)result.visitOrder.get(2).getId());
        assertEquals(1, (long)result.visitOrder.get(3).getId());
    }

    //    A1 ----> B2
//    |      / |
//    |    /   |
//    V  y     V
//    A3 ----> C4
    private static QueryGraph generateMockQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode A1 = new QueryNode(1);
        A1.addLabel(new QueryLabel("A"));

        QueryNode B2 = new QueryNode(2);
        B2.addLabel(new QueryLabel("B"));

        QueryNode A3 = new QueryNode(3);
        A3.addLabel(new QueryLabel("A"));

        QueryNode C4 = new QueryNode(4);
        C4.addLabel(new QueryLabel("C"));

        queryGraph.nodes.add(A1);
        queryGraph.nodes.add(B2);
        queryGraph.nodes.add(A3);
        queryGraph.nodes.add(C4);

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A1_B2 = A1.createRelationshipTo(B2, 1, RelTypes.KNOWS);
        Relationship A1_A3 = A1.createRelationshipTo(A3, 2, RelTypes.KNOWS);
        Relationship B2_A3 = B2.createRelationshipTo(A3, 3, RelTypes.KNOWS);
        Relationship B2_C4 = B2.createRelationshipTo(C4, 4, RelTypes.KNOWS);
        Relationship A3_C4 = A3.createRelationshipTo(C4, 5, RelTypes.KNOWS);

        queryGraph.relationships.add(A1_B2);
        queryGraph.relationships.add(A1_A3);
        queryGraph.relationships.add(B2_A3);
        queryGraph.relationships.add(B2_C4);
        queryGraph.relationships.add(A3_C4);

        return queryGraph;
    }
}