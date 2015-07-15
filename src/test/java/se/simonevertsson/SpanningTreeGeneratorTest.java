package se.simonevertsson;

import junit.framework.TestCase;
import se.simonevertsson.gpu.LabelDictionary;
import se.simonevertsson.gpu.SpanningTree;
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
        SpanningTreeGenerator spanningTreeGenerator = new SpanningTreeGenerator();

        // When
        SpanningTree result = spanningTreeGenerator.generate(queryGraph);

        // Then
        assertEquals(3, result.getRelationships().size());
        assertEquals(4, result.getVisitOrder().size());
        assertEquals(1, (long)result.getVisitOrder().get(0).getId());
        assertEquals(0, (long)result.getVisitOrder().get(1).getId());
        assertEquals(2, (long)result.getVisitOrder().get(2).getId());
        assertEquals(3, (long)result.getVisitOrder().get(3).getId());
    }

}