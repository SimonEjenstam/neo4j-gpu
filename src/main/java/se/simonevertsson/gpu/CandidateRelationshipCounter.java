package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;

import java.io.IOException;
import java.nio.IntBuffer;

public class CandidateRelationshipCounter {

    private final QueryKernels queryKernels;
    private final QueryBuffers queryBuffers;
    private final DataBuffers dataBuffers;
    private final int dataNodeCount;


    public CandidateRelationshipCounter(QueryKernels queryKernels, BufferContainer bufferContainer, int dataNodeCount) {
        this.queryKernels = queryKernels;
        this.queryBuffers = bufferContainer.queryBuffers;
        this.dataBuffers = bufferContainer.dataBuffers;
        this.dataNodeCount = dataNodeCount;
    }

    public Pointer<Integer> countCandidateRelationships(RelationshipCandidates relationshipCandidates) throws IOException {

        int[] candidateStartNodes = QueryUtils.gatherCandidateArray(
                this.queryBuffers.candidateIndicatorsPointer,
                this.dataNodeCount,
                (int) relationshipCandidates.getQueryStartNodeId());

        CLBuffer<Integer>
                candidateStartNodesBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);

        CLBuffer candidateRelationshipCounts = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, candidateStartNodes.length);


        int[] globalSizes = new int[]{(int) candidateStartNodes.length };

        CLEvent countRelationshipCandidatesEvent = this.queryKernels.countCandidateRelationshipsKernel.count_candidate_relationships(
                this.queryKernels.queue,
                relationshipCandidates.getQueryStartNodeId(),
                relationshipCandidates.getQueryEndNodeId(),
                this.dataBuffers.dataNodeRelationshipsBuffer,
                this.dataBuffers.dataRelationshipIndicesBuffer,
                candidateRelationshipCounts,
                candidateStartNodesBuffer,
                this.queryBuffers.candidateIndicatorsBuffer,
                this.dataNodeCount,
                globalSizes,
                null
        );

        relationshipCandidates.setCandidateStartNodes(candidateStartNodesBuffer);


        return candidateRelationshipCounts.read(this.queryKernels.queue, countRelationshipCandidatesEvent);
    }
}