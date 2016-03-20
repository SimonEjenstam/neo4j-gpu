package se.simonevertsson.gpu.query;

import junit.framework.TestCase;
import se.simonevertsson.MockHelper;
import se.simonevertsson.gpu.dictionary.LabelDictionary;
import se.simonevertsson.gpu.dictionary.TypeDictionary;
import se.simonevertsson.runner.QueryGraph;

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