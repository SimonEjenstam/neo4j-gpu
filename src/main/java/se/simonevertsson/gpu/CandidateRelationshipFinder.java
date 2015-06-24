package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;

import java.io.IOException;

public class CandidateRelationshipFinder {

    private final QueryKernels queryKernels;
    private final QueryBuffers queryBuffers;
    private final DataBuffers dataBuffers;
    private final int dataNodeCount;

    public CandidateRelationshipFinder(QueryKernels queryKernels, BufferContainer bufferContainer, int dataNodeCount) {
        this.queryKernels = queryKernels;
        this.queryBuffers = bufferContainer.queryBuffers;
        this.dataBuffers = bufferContainer.dataBuffers;
        this.dataNodeCount = dataNodeCount;
    }

    public CLBuffer<Integer> findCandidateRelationships(RelationshipCandidates relationshipCandidates) throws IOException {

        CLBuffer candidateEdgeEndNodes = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, relationshipCandidates.getTotalCount());


        int[] globalSizes = new int[]{(int) relationshipCandidates.getCandidateStartNodes().getElementCount()};

        CLEvent searchEdgeCandidatesEvent = this.queryKernels.searchEdgeCandidatesKernel.count_edge_candidates(
                this.queryKernels.queue,
                relationshipCandidates.getQueryStartNodeId(),
                relationshipCandidates.getQueryEndNodeId(),
                this.dataBuffers.dataNodeRelationshipsBuffer,
                this.dataBuffers.dataRelationshipIndicesBuffer,
                candidateEdgeEndNodes,
                relationshipCandidates.getCandidateEndNodeIndices(),
                relationshipCandidates.getCandidateStartNodes(),
                this.queryBuffers.candidateIndicatorsBuffer,
                this.dataNodeCount,
                globalSizes,
                null
        );

        searchEdgeCandidatesEvent.waitFor();
        relationshipCandidates.setCandidateEndNodes(candidateEdgeEndNodes);
        return candidateEdgeEndNodes;
    }
}