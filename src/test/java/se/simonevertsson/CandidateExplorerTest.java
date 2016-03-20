package se.simonevertsson;

import com.nativelibs4java.opencl.CLMem;
import junit.framework.TestCase;
import org.bridj.Pointer;
import org.neo4j.graphdb.Node;
import se.simonevertsson.gpu.query.candidate.initialization.CandidateExplorer;
import se.simonevertsson.gpu.graph.GpuGraph;
import se.simonevertsson.gpu.query.QueryUtils;

import java.io.IOException;

/**
 * Created by simon on 2015-06-23.
 */
public class CandidateExplorerTest extends TestCase {

    public void testExploreCandidatesYieldsCorrectResultWithQueryNode1() throws IOException {
        // Given
        boolean[] inputCandidateIndicators = {
                true, false, false, false,
                false, false, false, false,
                false, false, false, false,
                false, false, false, false,
        };



        MockQuery mockQuery = MockHelper.generateMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer =
                mockQuery.queryKernels.context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(inputCandidateIndicators), true);

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer =
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer.read(mockQuery.queryKernels.queue);


        CandidateExplorer candidateExplorer = new CandidateExplorer(mockQuery.queryKernels, mockQuery.bufferContainer, mockQuery.queryContext);
        GpuGraph query = mockQuery.queryContext.gpuQuery;

        Node queryNode = mockQuery.queryContext.queryGraph.nodes.get(0);

        int[] candidateArray = QueryUtils.gatherCandidateArray(
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer,
                mockQuery.queryContext.dataNodeCount,
                (int) queryNode.getId());

        // When
        candidateExplorer.exploreCandidates((int) queryNode.getId(), candidateArray);
        Pointer<Boolean> result = mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer;

        // Then
        boolean[] expectedCandidateIndicators = {
                true, false, false, false,
                false, true, false, false,
                false, false, true, false,
                false, false, false, false,
        };

        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateIndicators[i], (boolean) result.get(i));
        }
    }


    public void testExploreCandidatesYieldsCorrectResultWithQueryNode2() throws IOException {
        // Given
        boolean[] inputCandidateIndicators = {
                false, false, false, false,
                false, true, false, false,
                false, false, false, false,
                false, false, false, false,
        };



        MockQuery mockQuery = MockHelper.generateMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer =
                mockQuery.queryKernels.context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(inputCandidateIndicators), true);

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer =
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer.read(mockQuery.queryKernels.queue);


        CandidateExplorer candidateExplorer = new CandidateExplorer(mockQuery.queryKernels, mockQuery.bufferContainer, mockQuery.queryContext);
        GpuGraph query = mockQuery.queryContext.gpuQuery;

        Node queryNode = mockQuery.queryContext.queryGraph.nodes.get(1);

        int[] candidateArray = QueryUtils.gatherCandidateArray(
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer,
                mockQuery.queryContext.dataNodeCount,
                (int) queryNode.getId());

        // When
        candidateExplorer.exploreCandidates((int) queryNode.getId(), candidateArray);
        Pointer<Boolean> result = mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer;

        // Then
        boolean[] expectedCandidateIndicators = {
                false, false, false, false,
                false, true, false, false,
                false, false, true, false,
                false, false, false, true,
        };

        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateIndicators[i], (boolean) result.get(i));
        }
    }

    public void testExploreCandidatesYieldsCorrectResultWithQueryNode3() throws IOException {
        // Given
        boolean[] inputCandidateIndicators = {
                false, false, false, false,
                false, false, false, false,
                true, true, true, false,
                false, false, false, false,
        };



        MockQuery mockQuery = MockHelper.generateMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer =
                mockQuery.queryKernels.context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(inputCandidateIndicators), true);

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer =
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer.read(mockQuery.queryKernels.queue);


        CandidateExplorer candidateExplorer = new CandidateExplorer(mockQuery.queryKernels, mockQuery.bufferContainer, mockQuery.queryContext);
        GpuGraph query = mockQuery.queryContext.gpuQuery;

        Node queryNode = mockQuery.queryContext.queryGraph.nodes.get(2);

        int[] candidateArray = QueryUtils.gatherCandidateArray(
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer,
                mockQuery.queryContext.dataNodeCount,
                (int) queryNode.getId());

        // When
        candidateExplorer.exploreCandidates((int) queryNode.getId(), candidateArray);
        Pointer<Boolean> result = mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer;

        // Then
        boolean[] expectedCandidateIndicators = {
                false, false, false, false,
                false, false, false, false,
                false, true, true, false,
                false, false, false, true,
        };

        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateIndicators[i], (boolean) result.get(i));
        }
    }

    public void testExploreCandidatesYieldsCorrectResultWithQueryNode4() throws IOException {
        // Given
        boolean[] inputCandidateIndicators = {
                false, false, false, false,
                false, false, false, false,
                false, false, false, false,
                false, false, false, true,
        };



        MockQuery mockQuery = MockHelper.generateMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer =
                mockQuery.queryKernels.context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(inputCandidateIndicators), true);

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer =
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer.read(mockQuery.queryKernels.queue);


        CandidateExplorer candidateExplorer = new CandidateExplorer(mockQuery.queryKernels, mockQuery.bufferContainer, mockQuery.queryContext);
        GpuGraph query = mockQuery.queryContext.gpuQuery;

        Node queryNode = mockQuery.queryContext.queryGraph.nodes.get(3);

        int[] candidateArray = QueryUtils.gatherCandidateArray(
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer,
                mockQuery.queryContext.dataNodeCount,
                (int) queryNode.getId());

        // When
        candidateExplorer.exploreCandidates((int) queryNode.getId(), candidateArray);
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
