package se.simonevertsson;

import com.nativelibs4java.opencl.CLMem;
import junit.framework.TestCase;
import org.bridj.Pointer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.gpu.*;

import java.io.IOException;

/**
 * Created by simon on 2015-06-23.
 */
public class CandidateRelationshipCounterTest extends TestCase {

    public void testCountCandidateRelationshipsYieldsCorrectResultWithQueryRelationship1() throws IOException {

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


        CandidateRelationshipCounter candidateRelationshipCounter = new CandidateRelationshipCounter(mockQuery.queryKernels, mockQuery.bufferContainer, dataNodeCount);

        Relationship queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(0);

        RelationshipCandidates relationshipCandidates = new RelationshipCandidates(queryRelationship);

        // When
        Pointer<Integer> result =  candidateRelationshipCounter.countCandidateRelationships(relationshipCandidates);

        // Then
        int[] expectedCandidateRelationshipCounts = {
                2,1
        };

        for(int i = 0; i < expectedCandidateRelationshipCounts.length; i++) {
            assertEquals(expectedCandidateRelationshipCounts[i], (int) result.get(i));
        }
    }

    public void testCountCandidateRelationshipsYieldsCorrectResultWithQueryRelationship2() throws IOException {

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


        CandidateRelationshipCounter candidateRelationshipCounter = new CandidateRelationshipCounter(mockQuery.queryKernels, mockQuery.bufferContainer, dataNodeCount);

        Relationship queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(1);

        RelationshipCandidates relationshipCandidates = new RelationshipCandidates(queryRelationship);

        // When
        Pointer<Integer> result =  candidateRelationshipCounter.countCandidateRelationships(relationshipCandidates);

        // Then
        int[] expectedCandidateRelationshipCounts = {
                2,2
        };

        for(int i = 0; i < expectedCandidateRelationshipCounts.length; i++) {
            assertEquals(expectedCandidateRelationshipCounts[i], (int) result.get(i));
        }
    }

    public void testCountCandidateRelationshipsYieldsCorrectResultWithQueryRelationship3() throws IOException {

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


        CandidateRelationshipCounter candidateRelationshipCounter = new CandidateRelationshipCounter(mockQuery.queryKernels, mockQuery.bufferContainer, dataNodeCount);

        Relationship queryRelationship = mockQuery.queryContext.queryGraph.relationships.get(2);

        RelationshipCandidates relationshipCandidates = new RelationshipCandidates(queryRelationship);

        // When
        Pointer<Integer> result =  candidateRelationshipCounter.countCandidateRelationships(relationshipCandidates);

        // Then
        int[] expectedCandidateRelationshipCounts = {
                2,1
        };

        for(int i = 0; i < expectedCandidateRelationshipCounts.length; i++) {
            assertEquals(expectedCandidateRelationshipCounts[i], (int) result.get(i));
        }
    }

}
