package se.simonevertsson;

import junit.framework.TestCase;
import se.simonevertsson.gpu.GpuGraphModel;
import se.simonevertsson.gpu.GraphModelConverter;
import se.simonevertsson.gpu.LabelDictionary;
import se.simonevertsson.gpu.TypeDictionary;
import se.simonevertsson.query.QueryGraph;

import java.util.Arrays;

/**
 * Created by simon on 2015-05-12.
 */
public class GraphModelConverterTest extends TestCase {

    public void testGeneratesLabels() throws Exception {
        // Given
        QueryGraph queryGraph = MockHelper.generateMockQueryGraph();
        LabelDictionary labelDictionary = new LabelDictionary();
        TypeDictionary typeDictionary = new TypeDictionary();
        GraphModelConverter graphModelConverter = new GraphModelConverter(queryGraph.nodes, labelDictionary, typeDictionary);

        // When
        GpuGraphModel result = graphModelConverter.convert();

        // Then
        assertEquals(Arrays.toString(new int[] {1,2,-1,3}), Arrays.toString(result.getNodeLabels()));
        assertEquals(Arrays.toString(new int[] {0,1,2,3,4}), Arrays.toString(result.getLabelIndices()));
        assertEquals(Arrays.toString(new int[] {1,2,2,3,3,-1}), Arrays.toString(result.getNodeRelationships()));
        assertEquals(Arrays.toString(new int[] {-1,1,-1,2,-1,-1}), Arrays.toString(result.getRelationshipTypes()));
        assertEquals(Arrays.toString(new int[] {0,2,4,5,6}), Arrays.toString(result.getRelationshipIndices()));
    }
}