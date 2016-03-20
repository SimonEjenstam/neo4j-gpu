package se.simonevertsson;

import junit.framework.TestCase;
import se.simonevertsson.gpu.graph.GpuGraph;
import se.simonevertsson.gpu.graph.GpuGraphConverter;
import se.simonevertsson.gpu.query.dictionary.LabelDictionary;
import se.simonevertsson.gpu.query.dictionary.TypeDictionary;
import se.simonevertsson.runner.QueryGraph;

import java.util.Arrays;

public class GpuGraphConverterTest extends TestCase {

    public void testGeneratesCorrectLabels() throws Exception {
        // Given
        QueryGraph queryGraph = MockHelper.generateMockQueryGraph();
        LabelDictionary labelDictionary = new LabelDictionary();
        TypeDictionary typeDictionary = new TypeDictionary();
        GpuGraphConverter gpuGraphConverter = new GpuGraphConverter(queryGraph.nodes, labelDictionary, typeDictionary);

        // When
        GpuGraph result = gpuGraphConverter.convert();

        // Then
        assertEquals(Arrays.toString(new int[] {0,1,2,2,3}), Arrays.toString(result.getLabelIndices()));
        assertEquals(Arrays.toString(new int[] {1,2,3}), Arrays.toString(result.getNodeLabels()));
    }

    public void testGeneratesCorrectRelationships() throws Exception {
        // Given
        QueryGraph queryGraph = MockHelper.generateMockQueryGraph();
        LabelDictionary labelDictionary = new LabelDictionary();
        TypeDictionary typeDictionary = new TypeDictionary();
        GpuGraphConverter gpuGraphConverter = new GpuGraphConverter(queryGraph.nodes, labelDictionary, typeDictionary);

        // When
        GpuGraph result = gpuGraphConverter.convert();

        // Then
        assertEquals(Arrays.toString(new int[] {0,2,4,5,5}), Arrays.toString(result.getRelationshipIndices()));
        assertEquals(Arrays.toString(new int[] {1,2,2,3,3}), Arrays.toString(result.getNodeRelationships()));
        assertEquals(Arrays.toString(new int[] {-1,1,-1,2,-1}), Arrays.toString(result.getRelationshipTypes()));

    }
}