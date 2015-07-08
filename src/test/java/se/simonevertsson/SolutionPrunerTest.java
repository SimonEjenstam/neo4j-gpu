package se.simonevertsson;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLMem;
import junit.framework.TestCase;
import org.bridj.Pointer;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.gpu.CandidateRelationships;
import se.simonevertsson.gpu.QueryUtils;
import se.simonevertsson.gpu.SolutionPruner;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by simon on 2015-06-25.
 */
public class SolutionPrunerTest extends TestCase {
    private MockQuery mockQuery;
    private HashMap<Integer, CandidateRelationships> candidateRelationshipsHashMap;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        boolean[] inputCandidateIndicators = {
                true, true, false, false,
                false, true, true, false,
                false, true, true, true,
        };



        this.mockQuery = MockHelper.generateTriangleMockQuery();
        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer =
                mockQuery.queryKernels.context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(inputCandidateIndicators), true);
        mockQuery.bufferContainer.queryBuffers.candidateIndicatorsPointer =
                mockQuery.bufferContainer.queryBuffers.candidateIndicatorsBuffer.read(mockQuery.queryKernels.queue);

        this.candidateRelationshipsHashMap = new HashMap<Integer, CandidateRelationships>();

        /******* Relationship candidates 1 *********/

        Relationship queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(0);
        CandidateRelationships candidateRelationships = new CandidateRelationships(queryRelationship, this.mockQuery.queryKernels);

        int[] candidateStartNodes = {
                0,1
        };
        int[] candidateRelationshipEndNodeIndices = {
                0,2,3
        };
        int[] candidateRelationshipEndNodes = {
                1,2,2
        };

        CLBuffer<Integer>
                candidateStartNodesBuffer = mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);
        CLBuffer<Integer> candidateRelationshipEndNodeIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodeIndices), true);
        CLBuffer<Integer> candidateRelationshipEndNodesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodes), true);

        candidateRelationships.setCandidateStartNodes(candidateStartNodesBuffer);
        candidateRelationships.setCandidateEndNodeIndices(candidateRelationshipEndNodeIndicesBuffer);
        candidateRelationships.setEndNodeCount(candidateRelationshipEndNodeIndices[candidateRelationshipEndNodeIndices.length - 1]);
        candidateRelationships.setCandidateEndNodes(candidateRelationshipEndNodesBuffer);

        candidateRelationshipsHashMap.put((int) queryRelationship.getId(), candidateRelationships);

        /******* Relationship candidates 2 *********/

        queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(1);
        candidateRelationships = new CandidateRelationships(queryRelationship, this.mockQuery.queryKernels);

        candidateStartNodes =  new int[] {
                0,1
        };
        candidateRelationshipEndNodeIndices =  new int[] {
                0,2,4
        };
        candidateRelationshipEndNodes = new int[] {
                1,2,2,3
        };


        candidateStartNodesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);
        candidateRelationshipEndNodeIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodeIndices), true);
        candidateRelationshipEndNodesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodes), true);

        candidateRelationships.setCandidateStartNodes(candidateStartNodesBuffer);
        candidateRelationships.setCandidateEndNodeIndices(candidateRelationshipEndNodeIndicesBuffer);
        candidateRelationships.setEndNodeCount(candidateRelationshipEndNodeIndices[candidateRelationshipEndNodeIndices.length - 1]);
        candidateRelationships.setCandidateEndNodes(candidateRelationshipEndNodesBuffer);

        candidateRelationshipsHashMap.put((int) queryRelationship.getId(), candidateRelationships);

        /******* Relationship candidates 3 *********/

        queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(2);
        candidateRelationships = new CandidateRelationships(queryRelationship, this.mockQuery.queryKernels);

        candidateStartNodes =  new int[] {
                1,2
        };
        candidateRelationshipEndNodeIndices =  new int[] {
                0,2,3
        };
        candidateRelationshipEndNodes = new int[] {
                2,3,3
        };


        candidateStartNodesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);
        candidateRelationshipEndNodeIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodeIndices), true);
        candidateRelationshipEndNodesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodes), true);

        candidateRelationships.setCandidateStartNodes(candidateStartNodesBuffer);
        candidateRelationships.setCandidateEndNodeIndices(candidateRelationshipEndNodeIndicesBuffer);
        candidateRelationships.setEndNodeCount(candidateRelationshipEndNodeIndices[candidateRelationshipEndNodeIndices.length - 1]);
        candidateRelationships.setCandidateEndNodes(candidateRelationshipEndNodesBuffer);

        candidateRelationshipsHashMap.put((int) queryRelationship.getId(), candidateRelationships);
    }

    public void testPruneSolutionWithRelationship1WhenRelationship2And0Visited() throws IOException {
        // Given

        /* Relationship 2 and 0 visited */
        int[] possibleSolutions = {
                0,1,2, 0,1,3, 0,2,3, 1,2,3
        };

        boolean[] validationIndicators = {
                true, false, false, true
        };

        int[] outputIndicatorsArray = {
                0,1,1,1,2
        };

        CLBuffer<Integer> possibleSolutionsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(possibleSolutions), true);

        CLBuffer<Boolean> validationIndicatorsBuffer =
                mockQuery.queryKernels.context.createBuffer(CLMem.Usage.Input, Pointer.pointerToBooleans(validationIndicators), true);

        SolutionPruner solutionPruner = new SolutionPruner(this.mockQuery.queryKernels, this.mockQuery.queryContext);

        // When
        CLBuffer<Integer> result = solutionPruner.prunePossibleSolutions(possibleSolutionsBuffer, validationIndicatorsBuffer, outputIndicatorsArray);
        Pointer<Integer> resultPointer =result.read(this.mockQuery.queryKernels.queue);
        System.out.println(Arrays.toString(QueryUtils.pointerIntegerToArray(resultPointer, 6)));

        // Then
        int[] expectedPrunedPossibleSolutions = {
                0,1,2, 1,2,3
        };


        for(int i = 0; i < expectedPrunedPossibleSolutions.length; i++) {
            assertEquals(expectedPrunedPossibleSolutions[i], (int) resultPointer.get(i));
        }
    }

    public void testPruneSolutionWithRelationship2WhenRelationship0And1Visited() throws IOException {
        // Given

        /* Relationship  and 0 visited */
        int[] possibleSolutions = {
                0,1,1, 0,1,2, 0,2,1, 0,2,2, 1,2,2, 1,2,3
        };

        boolean[] validationIndicators = {
                false, true, false, false, false, true
        };

        int[] outputIndicatorsArray = {
                0,0,1,1,1,1,2
        };

        CLBuffer<Integer> possibleSolutionsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(possibleSolutions), true);

        CLBuffer<Boolean> validationIndicatorsBuffer =
                mockQuery.queryKernels.context.createBuffer(CLMem.Usage.Input, Pointer.pointerToBooleans(validationIndicators), true);

        SolutionPruner solutionPruner = new SolutionPruner(this.mockQuery.queryKernels, this.mockQuery.queryContext);

        // When
        CLBuffer<Integer> result = solutionPruner.prunePossibleSolutions(possibleSolutionsBuffer, validationIndicatorsBuffer, outputIndicatorsArray);
        Pointer<Integer> resultPointer =result.read(this.mockQuery.queryKernels.queue);
        System.out.println(Arrays.toString(QueryUtils.pointerIntegerToArray(resultPointer, 6)));

        // Then
        int[] expectedPrunedPossibleSolutions = {
                0,1,2, 1,2,3
        };


        for(int i = 0; i < expectedPrunedPossibleSolutions.length; i++) {
            assertEquals(expectedPrunedPossibleSolutions[i], (int) resultPointer.get(i));
        }
    }
}
