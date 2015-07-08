package se.simonevertsson;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLMem;
import junit.framework.TestCase;
import org.bridj.Pointer;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.gpu.CandidateRelationships;
import se.simonevertsson.gpu.QueryUtils;
import se.simonevertsson.gpu.SolutionCombinationCounter;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by simon on 2015-06-25.
 */
public class SolutionCombinationCounterTest extends TestCase {
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

    public void testcountSolutionCombinationsWithRelationship1AndStartNodeVisited() throws IOException {
        // Given

        /* Relationship 0 visited */
        int[] possibleSolutions = {
                0,1,-1, 0,2,-1, 1,2,-1
        };

        CLBuffer<Integer> possibleSolutionsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(possibleSolutions), true);

        CandidateRelationships candidateRelationships = this.candidateRelationshipsHashMap.get(1);

        SolutionCombinationCounter solutionCombinationCounter = new SolutionCombinationCounter(this.mockQuery.queryKernels, this.mockQuery.queryContext);

        // When
        Pointer<Integer> result =  solutionCombinationCounter.countSolutionCombinations(possibleSolutionsBuffer, candidateRelationships, true);
        System.out.println(Arrays.toString(QueryUtils.pointerIntegerToArray(result, 3)));

        // Then
        int[] expectedSolutionCombinationCounts = {
                2,2,2
        };


        for(int i = 0; i < expectedSolutionCombinationCounts.length; i++) {
            assertEquals(expectedSolutionCombinationCounts[i], (int) result.get(i));
        }
    }

    public void testcountSolutionCombinationsWithRelationship0AndStartNodeUnvisited() throws IOException {
        // Given

        /* Relationship 2 visited */
        int[] possibleSolutions = {
                -1,1,2, -1,1,3, -1,2,3
        };

        CLBuffer<Integer> possibleSolutionsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(possibleSolutions), true);

        CandidateRelationships candidateRelationships = this.candidateRelationshipsHashMap.get(0);

        SolutionCombinationCounter solutionCombinationCounter = new SolutionCombinationCounter(this.mockQuery.queryKernels, this.mockQuery.queryContext);

        // When
        Pointer<Integer> result =  solutionCombinationCounter.countSolutionCombinations(possibleSolutionsBuffer, candidateRelationships, false);
        System.out.println(Arrays.toString(QueryUtils.pointerIntegerToArray(result, 3)));

        // Then
        int[] expectedSolutionCombinationCounts = {
                1,1,2
        };


        for(int i = 0; i < expectedSolutionCombinationCounts.length; i++) {
            assertEquals(expectedSolutionCombinationCounts[i], (int) result.get(i));
        }
    }
}
