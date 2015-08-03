package se.simonevertsson;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLMem;
import junit.framework.TestCase;
import org.bridj.Pointer;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.gpu.CandidateRelationships;
import se.simonevertsson.gpu.PossibleSolutions;
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
        CandidateRelationships candidateRelationships = new CandidateRelationships(queryRelationship, mockQuery.queryContext.gpuQuery.getNodeIdDictionary(), this.mockQuery.queryKernels);

        int[] candidateStartNodes = {
                0,1
        };
        int[] candidateRelationshipEndNodeIndices = {
                0,2,3
        };
        int[] candidateRelationshipEndNodes = {
                1,2,2
        };

        int[] candidateRelationshipIndices = {
                0,1,2
        };

        CLBuffer<Integer>
                candidateStartNodesBuffer = mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);
        CLBuffer<Integer> candidateRelationshipEndNodeIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodeIndices), true);
        CLBuffer<Integer> candidateRelationshipEndNodesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodes), true);
        CLBuffer<Integer> candidateRelationshipIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipIndices), true);



        candidateRelationships.setCandidateStartNodes(candidateStartNodesBuffer);
        candidateRelationships.setCandidateEndNodeIndices(candidateRelationshipEndNodeIndicesBuffer);
        candidateRelationships.setEndNodeCount(candidateRelationshipEndNodeIndices[candidateRelationshipEndNodeIndices.length - 1]);
        candidateRelationships.setCandidateEndNodes(candidateRelationshipEndNodesBuffer);
        candidateRelationships.setCandidateRelationshipIndices(candidateRelationshipIndicesBuffer);

        candidateRelationshipsHashMap.put((int) queryRelationship.getId(), candidateRelationships);

        /******* Relationship candidates 2 *********/

        queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(1);
        candidateRelationships = new CandidateRelationships(queryRelationship, mockQuery.queryContext.gpuQuery.getNodeIdDictionary(), this.mockQuery.queryKernels);

        candidateStartNodes =  new int[] {
                0,1
        };
        candidateRelationshipEndNodeIndices =  new int[] {
                0,2,4
        };
        candidateRelationshipEndNodes = new int[] {
                1,2,2,3
        };

        candidateRelationshipIndices = new int[] {
                0,1,2,3
        };


        candidateStartNodesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);
        candidateRelationshipEndNodeIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodeIndices), true);
        candidateRelationshipEndNodesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodes), true);
        candidateRelationshipIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipIndices), true);

        candidateRelationships.setCandidateStartNodes(candidateStartNodesBuffer);
        candidateRelationships.setCandidateEndNodeIndices(candidateRelationshipEndNodeIndicesBuffer);
        candidateRelationships.setEndNodeCount(candidateRelationshipEndNodeIndices[candidateRelationshipEndNodeIndices.length - 1]);
        candidateRelationships.setCandidateEndNodes(candidateRelationshipEndNodesBuffer);
        candidateRelationships.setCandidateRelationshipIndices(candidateRelationshipIndicesBuffer);

        candidateRelationshipsHashMap.put((int) queryRelationship.getId(), candidateRelationships);

        /******* Relationship candidates 3 *********/

        queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(2);
        candidateRelationships = new CandidateRelationships(queryRelationship, mockQuery.queryContext.gpuQuery.getNodeIdDictionary(), this.mockQuery.queryKernels);

        candidateStartNodes =  new int[] {
                1,2
        };


        candidateRelationshipEndNodeIndices =  new int[] {
                0,2,3
        };
        candidateRelationshipEndNodes = new int[] {
                2,3,3
        };
        candidateRelationshipIndices = new int[] {
                2,3,4
        };


        candidateStartNodesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);
        candidateRelationshipEndNodeIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodeIndices), true);
        candidateRelationshipEndNodesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodes), true);
        candidateRelationshipIndicesBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipIndices), true);


        candidateRelationships.setCandidateStartNodes(candidateStartNodesBuffer);
        candidateRelationships.setCandidateEndNodeIndices(candidateRelationshipEndNodeIndicesBuffer);
        candidateRelationships.setEndNodeCount(candidateRelationshipEndNodeIndices[candidateRelationshipEndNodeIndices.length - 1]);
        candidateRelationships.setCandidateEndNodes(candidateRelationshipEndNodesBuffer);
        candidateRelationships.setCandidateRelationshipIndices(candidateRelationshipIndicesBuffer);

        candidateRelationshipsHashMap.put((int) queryRelationship.getId(), candidateRelationships);
    }

    public void testGenerateSolutionCombinationsWithRelationship0WhenRelationship2Visited() throws IOException {
        // Given

        /* Relationship 2 visited */
        int[] oldPossibleSolutionElements = {
                -1,1,2, -1,1,3, -1,2,3
        };


        int[] oldPossibleSolutionRelationships = {
                -1,-1, 2, -1,-1,3, -1,-1,4
        };

        CLBuffer<Integer> oldPossibleSolutionElementsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(oldPossibleSolutionElements), true);

        CLBuffer<Integer> oldPossibleSolutionRelationshipsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(oldPossibleSolutionRelationships), true);

        int[] solutionCombinationCounts = {
                1,1,2
        };

        CLBuffer<Integer> solutionCombinationCountsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(solutionCombinationCounts), true);


//        int[] combinationIndices = {
//                0,1,2,4
//        };

        PossibleSolutions oldPossibleSolutions = new PossibleSolutions(oldPossibleSolutionElementsBuffer, oldPossibleSolutionRelationshipsBuffer, this.mockQuery.queryContext, this.mockQuery.queryKernels.queue);

        int[] combinationIndices = QueryUtils.generatePrefixScanArray(solutionCombinationCountsBuffer.read(this.mockQuery.queryKernels.queue), 3);

        CandidateRelationships candidateRelationships = this.candidateRelationshipsHashMap.get(0);

        SolutionCombinationGenerator solutionCombinationGenerator = new SolutionCombinationGenerator(this.mockQuery.queryKernels, this.mockQuery.queryContext);

        // When
        PossibleSolutions result =  solutionCombinationGenerator.generateSolutionCombinations(oldPossibleSolutions, candidateRelationships, false, combinationIndices);
        Pointer<Integer> solutionElementsResultPointer =result.getSolutionElements().read(this.mockQuery.queryKernels.queue);
        Pointer<Integer> solutionRelationshipsResultPointer =result.getSolutionRelationships().read(this.mockQuery.queryKernels.queue);

        int solutionElementsSize = combinationIndices[combinationIndices.length-1]*mockQuery.queryContext.queryNodeCount;
        System.out.println(Arrays.toString(QueryUtils.pointerIntegerToArray(solutionElementsResultPointer, solutionElementsSize)));

        int solutionRelationshipsSize = combinationIndices[combinationIndices.length-1]*mockQuery.queryContext.queryRelationshipCount;
        System.out.println(Arrays.toString(QueryUtils.pointerIntegerToArray(solutionRelationshipsResultPointer, solutionRelationshipsSize)));

        // Then
        int[] expectedPossibleSolutionElements = {
                0,1,2, 0,1,3, 0,2,3, 1,2,3
        };

        for(int i = 0; i < expectedPossibleSolutionElements.length; i++) {
            assertEquals(expectedPossibleSolutionElements[i], (int) solutionElementsResultPointer.get(i));
        }

        int[] expectedPossibleSolutionRelationships = {
                0,-1,2, 0,-1,3, 1,-1,4, 2,-1,4
        };

        for(int i = 0; i < expectedPossibleSolutionRelationships.length; i++) {
            assertEquals(expectedPossibleSolutionRelationships[i], (int) solutionRelationshipsResultPointer.get(i));
        }
    }

    public void testGenerateSolutionCombinationsWithRelationship1WhenRelationship0Visited() throws IOException {
        // Given

        /* Relationship 0 visited */
        int[] oldPossibleSolutionElements = {
                0,1,-1, 0,2,-1, 1,2,-1
        };

        int[] oldPossibleSolutionRelationships = {
                0,-1,-1, 1,-1,-1, 2,-1,-1
        };

        CLBuffer<Integer> oldPossibleSolutionElementsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(oldPossibleSolutionElements), true);

        CLBuffer<Integer> oldPossibleSolutionRelationshipsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(oldPossibleSolutionRelationships), true);

        int[] solutionCombinationCounts = {
                1,1,1
        };

        CLBuffer<Integer> solutionCombinationCountsBuffer =
                mockQuery.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(solutionCombinationCounts), true);


//        int[] combinationIndices = {
//                0,2,4,6
//        };

        PossibleSolutions oldPossibleSolutions = new PossibleSolutions(oldPossibleSolutionElementsBuffer, oldPossibleSolutionRelationshipsBuffer, this.mockQuery.queryContext, this.mockQuery.queryKernels.queue);

        int[] combinationIndices = QueryUtils.generatePrefixScanArray(solutionCombinationCountsBuffer.read(this.mockQuery.queryKernels.queue), 3);

        CandidateRelationships candidateRelationships = this.candidateRelationshipsHashMap.get(1);

        SolutionCombinationGenerator solutionCombinationGenerator = new SolutionCombinationGenerator(this.mockQuery.queryKernels, this.mockQuery.queryContext);

        // When
        PossibleSolutions result =  solutionCombinationGenerator.generateSolutionCombinations(oldPossibleSolutions, candidateRelationships, true, combinationIndices);
        Pointer<Integer> solutionElementsResultPointer =result.getSolutionElements().read(this.mockQuery.queryKernels.queue);
        Pointer<Integer> solutionRelationshipsResultPointer =result.getSolutionRelationships().read(this.mockQuery.queryKernels.queue);

        int solutionElementsSize = combinationIndices[combinationIndices.length-1]*mockQuery.queryContext.queryNodeCount;
        System.out.println(Arrays.toString(QueryUtils.pointerIntegerToArray(solutionElementsResultPointer, solutionElementsSize)));

        int solutionRelationshipsSize = combinationIndices[combinationIndices.length-1]*mockQuery.queryContext.queryRelationshipCount;
        System.out.println(Arrays.toString(QueryUtils.pointerIntegerToArray(solutionRelationshipsResultPointer, solutionRelationshipsSize)));

        // Then
        int[] expectedPossibleSolutions = {
                0,1,2, 0,2,1, 1,2,3
        };

        for(int i = 0; i < expectedPossibleSolutions.length; i++) {
            assertEquals(expectedPossibleSolutions[i], (int) solutionElementsResultPointer.get(i));
        }

        int[] expectedPossibleSolutionRelationships = {
                0,1,-1, 1,0,-1, 2,3,-1
        };

        for(int i = 0; i < expectedPossibleSolutionRelationships.length; i++) {
            assertEquals(expectedPossibleSolutionRelationships[i], (int) solutionRelationshipsResultPointer.get(i));
        }
    }
}
