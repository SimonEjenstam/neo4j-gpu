package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;

import java.io.IOException;
import java.nio.IntBuffer;

public class CandidateExplorer {

    private final QueryKernels queryKernels;
    private final QueryBuffers queryBuffers;
    private final DataBuffers dataBuffers;
    private final QueryContext queryContext;

    public CandidateExplorer(QueryKernels queryKernels, BufferContainer bufferContainer, QueryContext queryContext) {
        this.queryKernels = queryKernels;
        this.queryBuffers = bufferContainer.queryBuffers;
        this.dataBuffers = bufferContainer.dataBuffers;
        this.queryContext = queryContext;
    }

    public void exploreCandidates(int queryNodeId, int[] candidateArray) throws IOException {
        int queryNodeAdjacencyIndexStart = this.queryContext.gpuQuery.getRelationshipIndices()[queryNodeId];
        int queryNodeAdjacencyIndexEnd =this.queryContext.gpuQuery.getRelationshipIndices()[queryNodeId + 1];

        CLBuffer<Integer> candidatesArray
                = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateArray), true);
        int[] globalSizes = new int[]{candidateArray.length};

        CLEvent exploreCandidatesEvent = this.queryKernels.exploreCandidatesKernel.explore_candidates(
                this.queryKernels.queue,
                queryNodeId,
                this.queryBuffers.queryNodeRelationshipsBuffer,
                this.queryBuffers.queryRelationshipTypesBuffer,
                this.queryBuffers.queryRelationshipIndicesBuffer,
                this.queryBuffers.queryNodeLabelsBuffer,
                this.queryBuffers.queryNodeLabelIndicesBuffer,
                queryNodeAdjacencyIndexStart,
                queryNodeAdjacencyIndexEnd,
                this.dataBuffers.dataNodeRelationshipsBuffer,
                this.dataBuffers.dataRelationshipTypesBuffer,
                this.dataBuffers.dataRelationshipIndicesBuffer,
                this.dataBuffers.dataLabelsBuffer,
                this.dataBuffers.dataLabelIndicesBuffer,
                candidatesArray,
                this.queryBuffers.candidateIndicatorsBuffer,
                this.queryContext.dataNodeCount,
                globalSizes,
                null
        );

        this.queryBuffers.candidateIndicatorsPointer =
                this.queryBuffers.candidateIndicatorsBuffer.read(this.queryKernels.queue, exploreCandidatesEvent);
    }
}