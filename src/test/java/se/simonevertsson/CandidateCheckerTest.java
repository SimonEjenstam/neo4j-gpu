package se.simonevertsson;

import junit.framework.TestCase;
import org.bridj.Pointer;
import org.neo4j.graphdb.Node;
import se.simonevertsson.gpu.CandidateChecker;
import se.simonevertsson.gpu.GpuGraph;

import java.io.IOException;

/**
 * Created by simon on 2015-06-23.
 */
public class CandidateCheckerTest extends TestCase {


    public void testYieldsCorrectOutputForMockQueryWithQueryNode1() throws IOException {
        // Given
        MockQuery mockQuery = MockHelper.generateMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;
        CandidateChecker candidateChecker = new CandidateChecker(mockQuery.queryContext, mockQuery.queryKernels, mockQuery.bufferContainer, dataNodeCount);
        GpuGraph query = mockQuery.queryContext.gpuQuery;

        Node queryNode = mockQuery.queryContext.queryGraph.nodes.get(0);

        // When
        candidateChecker.checkCandidates(query, queryNode);
        Pointer<Boolean> result = mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer;

        // Then
        boolean[] expectedCandidateIndicators = {
                true, false, false, false,
                false, false, false, false,
                false, false, false, false,
                false, false, false, false,
        };

        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateIndicators[i], (boolean) result.get(i));
        }
    }

    public void testYieldsCorrectOutputForMockQueryWithQueryNode2() throws IOException {
        // Given
        MockQuery mockQuery = MockHelper.generateMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;
        CandidateChecker candidateChecker = new CandidateChecker(mockQuery.queryContext, mockQuery.queryKernels, mockQuery.bufferContainer, dataNodeCount);
        GpuGraph query = mockQuery.queryContext.gpuQuery;

        Node queryNode = mockQuery.queryContext.queryGraph.nodes.get(1);

        // When
        candidateChecker.checkCandidates(query, queryNode);
        Pointer<Boolean> result = mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer;

        // Then
        boolean[] expectedCandidateIndicators = {
                false, false, false, false,
                false, true, false, false,
                false, false, false, false,
                false, false, false, false,
        };

        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateIndicators[i], (boolean) result.get(i));
        }
    }


    public void testYieldsCorrectOutputForMockQueryWithQueryNode3() throws IOException {
        // Given
        MockQuery mockQuery = MockHelper.generateMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;
        CandidateChecker candidateChecker = new CandidateChecker(mockQuery.queryContext, mockQuery.queryKernels, mockQuery.bufferContainer, dataNodeCount);
        GpuGraph query = mockQuery.queryContext.gpuQuery;

        Node queryNode = mockQuery.queryContext.queryGraph.nodes.get(2);

        // When
        candidateChecker.checkCandidates(query, queryNode);
        Pointer<Boolean> result = mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer;

        // Then
        boolean[] expectedCandidateIndicators = {
                false, false, false, false,
                false, false, false, false,
                true, true, true, false,
                false, false, false, false,
        };

        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateIndicators[i], (boolean) result.get(i));
        }
    }


    public void testYieldsCorrectOutputForMockQueryWithQueryNode4() throws IOException {
        // Given
        MockQuery mockQuery = MockHelper.generateMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;
        CandidateChecker candidateChecker = new CandidateChecker(mockQuery.queryContext, mockQuery.queryKernels, mockQuery.bufferContainer, dataNodeCount);
        GpuGraph query = mockQuery.queryContext.gpuQuery;

        Node queryNode = mockQuery.queryContext.queryGraph.nodes.get(3);

        // When
        candidateChecker.checkCandidates(query, queryNode);
        Pointer<Boolean> result = mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer;

        // Then
        boolean[] expectedCandidateIndicators = {
                false, false, false, false,
                false, false, false, false,
                false, false, false, false,
                false, false, false, true,
        };

        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateIndicators[i], (boolean) result.get(i));
        }
    }
}
