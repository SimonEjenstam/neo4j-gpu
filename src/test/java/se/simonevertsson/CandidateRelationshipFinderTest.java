package se.simonevertsson;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLMem;
import junit.framework.TestCase;
import org.bridj.Pointer;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.gpu.CandidateRelationshipFinder;
import se.simonevertsson.gpu.CandidateRelationships;

import java.io.IOException;
import java.nio.IntBuffer;

/**
 * Created by simon on 2015-06-25.
 */
public class CandidateRelationshipFinderTest extends TestCase {

    public void testFindCandidateRelationshipsYieldsCorrectResultWithQueryRelationship1() throws IOException {

        // Given
        boolean[] inputCandidateIndicators = {
                true, true, false, false,
                false, true, true, false,
                false, true, true, true,
        };



        MockQuery mockQuery = MockHelper.generateTriangleMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer =
                mockQuery.queryKernels.context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(inputCandidateIndicators), true);

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer =
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer.read(mockQuery.queryKernels.queue);

        Relationship queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(0);
        CandidateRelationships candidateRelationships = new CandidateRelationships(queryRelationship, mockQuery.queryContext.gpuQuery.getQueryIdDictionary(), mockQuery.queryKernels);

        int[] candidateStartNodes = {
                0,1
        };

        CLBuffer<Integer>
                candidateStartNodesBuffer = mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);

        int[] candidateRelationshipCounts = {
                2,1
        };

        int[] candidateRelationshipEndNodeIndices = {
                0,2,3
        };

        CLBuffer<Integer> candidateRelationshipEndNodeIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodeIndices), true);

        candidateRelationships.setCandidateStartNodes(candidateStartNodesBuffer);
        candidateRelationships.setCandidateEndNodeIndices(candidateRelationshipEndNodeIndicesBuffer);
        candidateRelationships.setEndNodeCount(candidateRelationshipEndNodeIndices[candidateRelationshipEndNodeIndices.length - 1]);



        CandidateRelationshipFinder candidateRelationshipFinder = new CandidateRelationshipFinder(mockQuery.queryKernels, mockQuery.bufferContainer, dataNodeCount);





        // When
        CLBuffer<Integer> result =  candidateRelationshipFinder.findCandidateRelationships(candidateRelationships);
        Pointer<Integer> resultPointer = result.read(mockQuery.queryKernels.queue);

        // Then
        int[] expectedCandidateRelationshipEndNodes = {
                1,2,2
        };

        for(int i = 0; i < expectedCandidateRelationshipEndNodes.length; i++) {
            assertEquals(expectedCandidateRelationshipEndNodes[i], (int) resultPointer.get(i));
        }
    }

    public void testFindCandidateRelationshipsYieldsCorrectResultWithQueryRelationship2() throws IOException {

        // Given
        boolean[] inputCandidateIndicators = {
                true, true, false, false,
                false, true, true, false,
                false, true, true, true,
        };



        MockQuery mockQuery = MockHelper.generateTriangleMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer =
                mockQuery.queryKernels.context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(inputCandidateIndicators), true);

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer =
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer.read(mockQuery.queryKernels.queue);

        Relationship queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(1);
        CandidateRelationships candidateRelationships = new CandidateRelationships(queryRelationship, mockQuery.queryContext.gpuQuery.getQueryIdDictionary(), mockQuery.queryKernels);

        int[] candidateStartNodes = {
                0,1
        };

        CLBuffer<Integer>
                candidateStartNodesBuffer = mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);

        int[] candidateRelationshipCounts = {
                2,2
        };

        int[] candidateRelationshipEndNodeIndices = {
                0,2,4
        };

        CLBuffer<Integer> candidateRelationshipEndNodeIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodeIndices), true);

        candidateRelationships.setCandidateStartNodes(candidateStartNodesBuffer);
        candidateRelationships.setCandidateEndNodeIndices(candidateRelationshipEndNodeIndicesBuffer);
        candidateRelationships.setEndNodeCount(candidateRelationshipEndNodeIndices[candidateRelationshipEndNodeIndices.length - 1]);



        CandidateRelationshipFinder candidateRelationshipFinder = new CandidateRelationshipFinder(mockQuery.queryKernels, mockQuery.bufferContainer, dataNodeCount);





        // When
        CLBuffer<Integer> result =  candidateRelationshipFinder.findCandidateRelationships(candidateRelationships);
        Pointer<Integer> resultPointer = result.read(mockQuery.queryKernels.queue);

        // Then
        int[] expectedCandidateRelationshipEndNodes = {
                1,2,2,3
        };

        for(int i = 0; i < expectedCandidateRelationshipEndNodes.length; i++) {
            assertEquals(expectedCandidateRelationshipEndNodes[i], (int) resultPointer.get(i));
        }
    }

    public void testFindCandidateRelationshipsYieldsCorrectResultWithQueryRelationship3() throws IOException {

        // Given
        boolean[] inputCandidateIndicators = {
                true, true, false, false,
                false, true, true, false,
                false, true, true, true,
        };



        MockQuery mockQuery = MockHelper.generateTriangleMockQuery();

        int dataNodeCount = mockQuery.queryContext.dataNodeCount;
        int queryNodeCount = mockQuery.queryContext.queryNodeCount;

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer =
                mockQuery.queryKernels.context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(inputCandidateIndicators), true);

        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer =
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer.read(mockQuery.queryKernels.queue);

        Relationship queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(2);
        CandidateRelationships candidateRelationships = new CandidateRelationships(queryRelationship, mockQuery.queryContext.gpuQuery.getQueryIdDictionary(), mockQuery.queryKernels);

        int[] candidateStartNodes = {
                1,2
        };

        CLBuffer<Integer>
                candidateStartNodesBuffer = mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);

        int[] candidateRelationshipCounts = {
                2,1
        };

        int[] candidateRelationshipEndNodeIndices = {
                0,2,3
        };

        CLBuffer<Integer> candidateRelationshipEndNodeIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodeIndices), true);

        candidateRelationships.setCandidateStartNodes(candidateStartNodesBuffer);
        candidateRelationships.setCandidateEndNodeIndices(candidateRelationshipEndNodeIndicesBuffer);
        candidateRelationships.setEndNodeCount(candidateRelationshipEndNodeIndices[candidateRelationshipEndNodeIndices.length - 1]);



        CandidateRelationshipFinder candidateRelationshipFinder = new CandidateRelationshipFinder(mockQuery.queryKernels, mockQuery.bufferContainer, dataNodeCount);

        
        // When
        CLBuffer<Integer> result =  candidateRelationshipFinder.findCandidateRelationships(candidateRelationships);
        Pointer<Integer> resultPointer = result.read(mockQuery.queryKernels.queue);

        // Then
        int[] expectedCandidateRelationshipEndNodes = {
                2,3,3
        };

        for(int i = 0; i < expectedCandidateRelationshipEndNodes.length; i++) {
            assertEquals(expectedCandidateRelationshipEndNodes[i], (int) resultPointer.get(i));
        }
    }
}
