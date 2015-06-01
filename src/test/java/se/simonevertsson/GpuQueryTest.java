package se.simonevertsson;

import junit.framework.TestCase;
import org.bridj.Pointer;
import se.simonevertsson.gpu.GpuGraphModel;
import se.simonevertsson.gpu.GpuQuery;
import se.simonevertsson.gpu.GraphModelConverter;
import se.simonevertsson.gpu.LabelDictionary;
import se.simonevertsson.query.QueryGraph;

/**
 * Created by simon on 2015-06-01.
 */
public class GpuQueryTest extends TestCase {

//    public void testCheckCandidates() throws Exception {
//        // Given
//        QueryGraph queryGraph = MockHelper.generateMockQueryGraph();
//        LabelDictionary dictionary = new LabelDictionary();
//        GraphModelConverter converter = new GraphModelConverter(queryGraph.nodes, dictionary);
//        GpuGraphModel queryGraphModel = converter.convert();
//        GpuQuery gpuQuery = new GpuQuery();
//
//        int dataNodeCount = 4;
//        int queryNodeCount = 4;
//        int[] globalSizes = new int[] { dataNodeCount };
//        ;
//
//        boolean[] candidateSet = {
//                false, true, false, false,
//                false, false, false, false,
//                false, false, false, false,
//                false, false, false, false,
//        };
//
//        int[] expectedResult = new int[] {
//                0, 0, 1, 1
//        };
//
//        Pointer<Boolean> c_set = Pointer.pointerToBooleans(candidateSet);
//
//        // When
//        int[] result = gpuQuery.checkCandidates(0, queryGraphModel, dataNodeCount, globalSizes, )
//
//
//
//
//        // Then
//        assertEquals(1, result.length);
//        assertEquals(1, result[0]);
//    }


    public void testGatherCandidates() throws Exception {
        // Given
        GpuQuery gpuQuery = new GpuQuery(null, null);

        int dataNodeCount = 4;

        boolean[] candidateSet = {
                false, true, false, false,
                false, false, false, false,
                false, false, false, false,
                false, false, false, false,
        };

        int[] expectedResult = new int[] {
                0, 0, 1, 1
        };

        Pointer<Boolean> c_set = Pointer.pointerToBooleans(candidateSet);

        // When
        int[] result = gpuQuery.gatherCandidateArray(0, c_set, dataNodeCount);




        // Then
        assertEquals(1, result.length);
        assertEquals(1, result[0]);
    }
}
