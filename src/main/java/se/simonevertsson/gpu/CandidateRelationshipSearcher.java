package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;

public class CandidateRelationshipSearcher {

    private final QueryContext queryContext;
    private final QueryBuffers queryBuffers;
    private final int dataNodeCount;
    private final CandidateRelationshipCounter candidateRelationshipCounter;
    private final QueryKernels queryKernels;
    private final CandidateRelationshipFinder candidateRelationshipFinder;

    public CandidateRelationshipSearcher(QueryContext queryContext, QueryKernels queryKernels, BufferContainer bufferContainer) {
        this.queryBuffers = bufferContainer.queryBuffers;
        this.queryContext = queryContext;
        this.queryKernels = queryKernels;
        this.dataNodeCount = queryContext.dataNodeCount;
        this.candidateRelationshipCounter = new CandidateRelationshipCounter(queryKernels, bufferContainer, this.dataNodeCount);
        this.candidateRelationshipFinder = new CandidateRelationshipFinder(queryKernels, bufferContainer, this.dataNodeCount);
    }

    HashMap<Integer, RelationshipCandidates> searchCandidateRelationships() throws IOException {
        ArrayList<Relationship> relationships = this.queryContext.queryGraph.relationships;
        HashMap<Integer, RelationshipCandidates> edgeCandidatesHashMap = new HashMap<Integer, RelationshipCandidates>();

        for (Relationship relationship : relationships) {
            RelationshipCandidates relationshipCandidates = new RelationshipCandidates(relationship);

            Pointer<Integer> candidateRelationshipCountsPointer = this.candidateRelationshipCounter.countCandidateRelationships(relationshipCandidates);

            int[] candidateRelationshipEndNodeIndices = QueryUtils.generatePrefixScanArray(candidateRelationshipCountsPointer, relationshipCandidates.getStartNodeCount());
            CLBuffer<Integer>
                    candidateRelationshipEndNodeIndicesBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateRelationshipEndNodeIndices), true);
            relationshipCandidates.setCandidateEndNodeIndicies(candidateRelationshipEndNodeIndicesBuffer);

            this.candidateRelationshipFinder.findCandidateRelationships(relationshipCandidates);
            edgeCandidatesHashMap.put((int) relationship.getId(), relationshipCandidates);

        }

        return edgeCandidatesHashMap;
    }

}