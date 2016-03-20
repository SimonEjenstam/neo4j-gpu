package se.simonevertsson.gpu.query.candidate.refinement;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import se.simonevertsson.gpu.query.QueryContext;
import se.simonevertsson.gpu.buffer.BufferContainer;
import se.simonevertsson.gpu.buffer.DataBuffers;
import se.simonevertsson.gpu.buffer.QueryBuffers;
import se.simonevertsson.gpu.kernel.QueryKernels;

import java.io.IOException;
import java.nio.IntBuffer;

public class CandidateRefinery {
  private final QueryKernels queryKernels;
  private final QueryBuffers queryBuffers;
  private final DataBuffers dataBuffers;
  private final QueryContext queryContext;
  private final int dataNodeCount;

  public CandidateRefinery(QueryKernels queryKernels, BufferContainer bufferContainer, QueryContext queryContext) {
    this.queryKernels = queryKernels;
    this.queryBuffers = bufferContainer.queryBuffers;
    this.dataBuffers = bufferContainer.dataBuffers;
    this.queryContext = queryContext;
    this.dataNodeCount = queryContext.dataNodeCount;

  }

  public void refineCandidates(int queryNodeId, int[] candidateArray) throws IOException {
    int queryNodeAdjacencyIndexStart = this.queryContext.gpuQuery.getRelationshipIndices()[queryNodeId];
    int queryNodeAdjacencyIndexEnd = this.queryContext.gpuQuery.getRelationshipIndices()[queryNodeId + 1];

    IntBuffer candidatesArrayBuffer = IntBuffer.wrap(candidateArray);
    CLBuffer<Integer> candidatesArray
        = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, candidatesArrayBuffer, true);
    int[] globalSizes = new int[]{candidateArray.length};

    CLEvent refineCandidatesEvent = this.queryKernels.refineCandidatesKernel.refine_candidates(
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

    this.queryBuffers.candidateIndicatorsPointer = this.queryBuffers.candidateIndicatorsBuffer.read(this.queryKernels.queue, refineCandidatesEvent);
  }
}