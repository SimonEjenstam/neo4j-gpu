package se.simonevertsson;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLMem;
import junit.framework.TestCase;
import org.bridj.Pointer;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.gpu.CandidateRelationships;
import se.simonevertsson.gpu.QueryUtils;
import se.simonevertsson.gpu.SolutionCombinationGenerator;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by simon on 2015-06-25.
 */
public class SolutionCombinationGeneratorTest extends TestCase {
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

    public void testGenerateSolutionCombinationsWithRelationship0WhenRelationship2Visited() throws IOException {
        // Given

        /* Relationship 2 visited */
        int[] oldPossibleSolutions = {
                -1,1,2, -1,1,3, -1,2,3
        };

        CLBuffer<Integer> oldPossibleSolutionsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(oldPossibleSolutions), true);

        int[] solutionCombinationCounts = {
                1,1,2
        };

        CLBuffer<Integer> solutionCombinationCountsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(solutionCombinationCounts), true);


        int[] combinationIndices = {
                0,1,2,4
        };

        //combinationIndices = QueryUtils.generatePrefixScanArray(solutionCombinationCountsBuffer.read(this.mockQuery.queryKernels.queue), 3);

        CandidateRelationships candidateRelationships = this.candidateRelationshipsHashMap.get(0);

        SolutionCombinationGenerator solutionCombinationGenerator = new SolutionCombinationGenerator(this.mockQuery.queryKernels, this.mockQuery.queryContext);

        // When
        CLBuffer<Integer> result =  solutionCombinationGenerator.generateSolutionCombinations(oldPossibleSolutionsBuffer, candidateRelationships, false, combinationIndices);
        Pointer<Integer> resultPointer =result.read(this.mockQuery.queryKernels.queue);
        System.out.println(Arrays.toString(QueryUtils.pointerIntegerToArray(resultPointer, 12)));

        // Then
        int[] expectedPossibleSolutions = {
                0,1,2, 0,1,3, 0,2,3, 1,2,3
        };


        for(int i = 0; i < expectedPossibleSolutions.length; i++) {
            assertEquals(expectedPossibleSolutions[i], (int) resultPointer.get(i));
        }
    }

    public void testGenerateSolutionCombinationsWithRelationship1WhenRelationship0Visited() throws IOException {
        // Given

        /* Relationship 0 visited */
        int[] oldPossibleSolutions = {
                0,1,-1, 0,2,-1, 1,2,-1
        };

        CLBuffer<Integer> oldPossibleSolutionsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(oldPossibleSolutions), true);

        int[] solutionCombinationCounts = {
                2,2,2
        };

        CLBuffer<Integer> solutionCombinationCountsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(solutionCombinationCounts), true);


        int[] combinationIndices = {
                0,2,4,6
        };

        //combinationIndices = QueryUtils.generatePrefixScanArray(solutionCombinationCountsBuffer.read(this.mockQuery.queryKernels.queue), 3);

        CandidateRelationships candidateRelationships = this.candidateRelationshipsHashMap.get(1);

        SolutionCombinationGenerator solutionCombinationGenerator = new SolutionCombinationGenerator(this.mockQuery.queryKernels, this.mockQuery.queryContext);

        // When
        CLBuffer<Integer> result =  solutionCombinationGenerator.generateSolutionCombinations(oldPossibleSolutionsBuffer, candidateRelationships, true, combinationIndices);
        Pointer<Integer> resultPointer =result.read(this.mockQuery.queryKernels.queue);
        System.out.println(Arrays.toString(QueryUtils.pointerIntegerToArray(resultPointer, 18)));

        // Then
        int[] expectedPossibleSolutions = {
                0,1,1, 0,1,2, 0,2,1, 0,2,2, 1,2,2, 1,2,3
        };


        for(int i = 0; i < expectedPossibleSolutions.length; i++) {
            assertEquals(expectedPossibleSolutions[i], (int) resultPointer.get(i));
        }
    }
}
