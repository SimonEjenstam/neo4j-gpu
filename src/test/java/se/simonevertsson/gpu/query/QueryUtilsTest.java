package se.simonevertsson.gpu.query;

import junit.framework.TestCase;
import org.bridj.Pointer;
import se.simonevertsson.gpu.query.QueryUtils;

/**
 * Created by simon on 2015-06-01.
 */
public class QueryUtilsTest extends TestCase {

    public void testGatherCandidates() throws Exception {
        // Given

        int dataNodeCount = 4;

        boolean[] candidateIndicators = {
                false, true, false, false,
                false, false, false, false,
                false, false, false, false,
                false, false, false, false,
        };

        int[] expectedResult = new int[] {
                1
        };

        int nodeId = 0;

        Pointer<Boolean> candidateIndicatorsPointer = Pointer.pointerToBooleans(candidateIndicators);

        // When
        int[] result = QueryUtils.gatherCandidateArray(candidateIndicatorsPointer, dataNodeCount, nodeId);




        // Then
        for(int i = 0; i < expectedResult.length; i++) {
            assertEquals(expectedResult[i], result[i]);
        }
    }
}
