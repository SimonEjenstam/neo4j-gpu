package se.simonevertsson;

import junit.framework.TestCase;
import se.simonevertsson.gpu.LabelDictionary;
import se.simonevertsson.gpu.SpanningTreeGenerator;
import se.simonevertsson.gpu.TypeDictionary;
import se.simonevertsson.query.QueryGraph;

import java.lang.reflect.Type;

/**
 * Created by simon on 2015-05-12.
 */
public class SpanningTreeGeneratorTest extends TestCase {

    public void testGenerateQueryGraph() throws Exception {
        // Given
        QueryGraph queryGraph = MockHelper.generateBasicMockQueryGraph();
        LabelDictionary labelDictionary = new LabelDictionary();
        TypeDictionary typeDictionary = new TypeDictionary();
        SpanningTreeGenerator spanningTreeGenerator = new SpanningTreeGenerator(queryGraph, labelDictionary, typeDictionary);

        // When
        QueryGraph result = spanningTreeGenerator.generateQueryGraph();

        // Then
        assertEquals(3, result.spanningTree.size());
        assertEquals(4, result.visitOrder.size());
        assertEquals(1, (long)result.visitOrder.get(0).getId());
        assertEquals(0, (long)result.visitOrder.get(1).getId());
        assertEquals(2, (long)result.visitOrder.get(2).getId());
        assertEquals(3, (long)result.visitOrder.get(3).getId());
    }

}