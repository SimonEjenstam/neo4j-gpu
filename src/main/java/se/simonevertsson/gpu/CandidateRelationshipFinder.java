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

    public CLBuffer<Integer> findCandidateRelationships(CandidateRelationships candidateRelationships) throws IOException {

        CLBuffer candidateRelationshipEndNodes = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, candidateRelationships.getTotalCount());


        int[] globalSizes = new int[]{(int) candidateRelationships.getStartNodeCount()};

        CLEvent findCandidateRelationshipsEvent = this.queryKernels.findCandidateRelationshipsKernel.find_candidate_relationships(
                this.queryKernels.queue,

                candidateRelationships.getQueryStartNodeId(),
                candidateRelationships.getQueryEndNodeId(),
                this.dataBuffers.dataNodeRelationshipsBuffer,
                this.dataBuffers.dataRelationshipIndicesBuffer,

                candidateRelationshipEndNodes,
                candidateRelationships.getCandidateEndNodeIndices(),

                candidateRelationships.getCandidateStartNodes(),
                this.queryBuffers.candidateIndicatorsBuffer,
                this.dataNodeCount,

                globalSizes,
                null
        );

        findCandidateRelationshipsEvent.waitFor();
        candidateRelationships.setCandidateEndNodes(candidateRelationshipEndNodes);
        return candidateRelationshipEndNodes;
    }
}