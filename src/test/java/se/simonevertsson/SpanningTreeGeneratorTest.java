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

    public void testGenerateQueryGraph() throws Exception {
        // Given
        QueryGraph queryGraph = MockHelper.generateMockQueryGraph();
        LabelDictionary labelDictionary = new LabelDictionary();
        SpanningTreeGenerator spanningTreeGenerator = new SpanningTreeGenerator(queryGraph, labelDictionary);

        // When
        QueryGraph result = spanningTreeGenerator.generateQueryGraph();

        // Then
        assertEquals(3, result.spanningTree.size());
        assertEquals(4, result.visitOrder.size());
        assertEquals(2, (long)result.visitOrder.get(0).getId());
        assertEquals(1, (long)result.visitOrder.get(1).getId());
        assertEquals(3, (long)result.visitOrder.get(2).getId());
        assertEquals(4, (long)result.visitOrder.get(3).getId());
    }

}