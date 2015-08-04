package se.simonevertsson;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLMem;
import junit.framework.TestCase;
import org.bridj.Pointer;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.gpu.CandidateRelationships;
import se.simonevertsson.gpu.PossibleSolutions;
import se.simonevertsson.gpu.SolutionInitializer;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by simon on 2015-06-25.
 */
public class SolutionInitializerTest extends TestCase{

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
        CandidateRelationships candidateRelationships = new CandidateRelationships(queryRelationship, mockQuery.queryContext, this.mockQuery.queryKernels);

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
        candidateRelationships = new CandidateRelationships(queryRelationship, mockQuery.queryContext, this.mockQuery.queryKernels);

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
        candidateRelationships = new CandidateRelationships(queryRelationship, mockQuery.queryContext, this.mockQuery.queryKernels);

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

    public void testFindQueryRelationshipIdWithMinimumCandidates() throws IOException {
        // Given
        SolutionInitializer solutionInitializer = new SolutionInitializer(this.mockQuery.queryContext, this.mockQuery.queryKernels);

        // When
        int result =  solutionInitializer.findQueryRelationshipIdWithMinimumCandidates(this.candidateRelationshipsHashMap);

        // Then
        assertEquals(2, result);
    }

    public void testCreateInitialPossibleSolutionsWithRelationship0() throws IOException {
        // Given
        SolutionInitializer solutionInitializer = new SolutionInitializer(this.mockQuery.queryContext, this.mockQuery.queryKernels);

        // When
        PossibleSolutions result =  solutionInitializer.createInitialPossibleSolutions(this.candidateRelationshipsHashMap.get(0));

        // Then
        int[] expectedInitialPossibleSolutionElements = {
            0,1,-1, 0,2,-1, 1,2,-1
        };

        int[] expectedInitialPossibleSolutionRelationships = {
                0,-1,-1, 1,-1,-1, 2,-1,-1
        };



        Pointer<Integer> solutionElementsResultPointer = result.getSolutionElements().read(this.mockQuery.queryKernels.queue);
        for(int i = 0; i < expectedInitialPossibleSolutionElements.length; i++) {
            assertEquals(expectedInitialPossibleSolutionElements[i], (int) solutionElementsResultPointer.get(i));
        }

        Pointer<Integer> solutionRelationshipsResultPointer = result.getSolutionRelationships().read(this.mockQuery.queryKernels.queue);
        for(int i = 0; i < expectedInitialPossibleSolutionRelationships.length; i++) {
            assertEquals(expectedInitialPossibleSolutionRelationships[i], (int) solutionRelationshipsResultPointer.get(i));
        }
    }

    public void testCreateInitialPossibleSolutionsWithRelationship1() throws IOException {
        // Given
        SolutionInitializer solutionInitializer = new SolutionInitializer(this.mockQuery.queryContext, this.mockQuery.queryKernels);

        // When
        PossibleSolutions result =  solutionInitializer.createInitialPossibleSolutions(this.candidateRelationshipsHashMap.get(1));

        // Then
        int[] expectedInitialPossibleSolutions = {
                0,-1,1, 0,-1,2, 1,-1,2, 1,-1,3
        };

        int[] expectedInitialPossibleSolutionRelationships = {
                -1,0,-1, -1,1,-1, -1,2,-1, -1,3,-1
        };

        Pointer<Integer> resultPointer = result.getSolutionElements().read(this.mockQuery.queryKernels.queue);
        for(int i = 0; i < expectedInitialPossibleSolutions.length; i++) {
            assertEquals(expectedInitialPossibleSolutions[i], (int) resultPointer.get(i));
        }

        Pointer<Integer> solutionRelationshipsResultPointer = result.getSolutionRelationships().read(this.mockQuery.queryKernels.queue);
        for(int i = 0; i < expectedInitialPossibleSolutionRelationships.length; i++) {
            assertEquals(expectedInitialPossibleSolutionRelationships[i], (int) solutionRelationshipsResultPointer.get(i));
        }
    }

    public void testCreateInitialPossibleSolutionsWithRelationship2() throws IOException {
        // Given
        SolutionInitializer solutionInitializer = new SolutionInitializer(this.mockQuery.queryContext, this.mockQuery.queryKernels);

        // When
        PossibleSolutions result =  solutionInitializer.createInitialPossibleSolutions(this.candidateRelationshipsHashMap.get(2));

        // Then
        int[] expectedInitialPossibleSolutions = {
                -1,1,2, -1,1,3, -1,2,3
        };

        int[] expectedInitialPossibleSolutionRelationships = {
                -1,-1, 2, -1,-1,3, -1,-1,4
        };

        Pointer<Integer> resultPointer = result.getSolutionElements().read(this.mockQuery.queryKernels.queue);
        for(int i = 0; i < expectedInitialPossibleSolutions.length; i++) {
            assertEquals(expectedInitialPossibleSolutions[i], (int) resultPointer.get(i));
        }

        Pointer<Integer> solutionRelationshipsResultPointer = result.getSolutionRelationships().read(this.mockQuery.queryKernels.queue);
        for(int i = 0; i < expectedInitialPossibleSolutionRelationships.length; i++) {
            assertEquals(expectedInitialPossibleSolutionRelationships[i], (int) solutionRelationshipsResultPointer.get(i));
        }

    }


    public void testInitializePossibleSolution() throws IOException {
        // Given
        SolutionInitializer solutionInitializer = new SolutionInitializer(this.mockQuery.queryContext, this.mockQuery.queryKernels);
        ArrayList<Integer> visitedRelationships = new ArrayList<Integer>();
        ArrayList<Integer> visitedNodes = new ArrayList<Integer>();

        // When
        PossibleSolutions result =  solutionInitializer.initializePossibleSolutions(candidateRelationshipsHashMap, visitedRelationships, visitedNodes);
        Pointer<Integer> solutionElementsResultPointer = result.getSolutionElements().read(this.mockQuery.queryKernels.queue);

        // Then
        assertEquals(1, visitedRelationships.size());
        assertEquals(2, visitedNodes.size());
        assertTrue(visitedRelationships.contains(2));
        assertTrue(visitedNodes.contains(1));
        assertTrue(visitedNodes.contains(2));

        int[] expectedInitialPossibleSolutions = {
                -1,1,2, -1,1,3, -1,2,3
        };

        int[] expectedInitialPossibleSolutionRelationships = {
                -1,-1, 2, -1,-1,3, -1,-1,4
        };

        for(int i = 0; i < expectedInitialPossibleSolutions.length; i++) {
            assertEquals(expectedInitialPossibleSolutions[i], (int) solutionElementsResultPointer.get(i));
        }

        Pointer<Integer> solutionRelationshipsResultPointer = result.getSolutionRelationships().read(this.mockQuery.queryKernels.queue);
        for(int i = 0; i < expectedInitialPossibleSolutionRelationships.length; i++) {
            assertEquals(expectedInitialPossibleSolutionRelationships[i], (int) solutionRelationshipsResultPointer.get(i));
        }
    }
}
