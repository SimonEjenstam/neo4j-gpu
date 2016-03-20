package se.simonevertsson.gpu.query.relationship.search;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;
import se.simonevertsson.gpu.query.QueryUtils;
import se.simonevertsson.gpu.buffer.BufferContainer;
import se.simonevertsson.gpu.buffer.DataBuffers;
import se.simonevertsson.gpu.buffer.QueryBuffers;
import se.simonevertsson.gpu.kernel.QueryKernels;

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

  public Pointer<Integer> countCandidateRelationships(CandidateRelationships candidateRelationships) throws IOException {

    int[] candidateStartNodes = QueryUtils.gatherCandidateArray(
        this.queryBuffers.candidateIndicatorsPointer,
        this.dataNodeCount,
        (int) candidateRelationships.getQueryStartNodeId());

    CLBuffer<Integer>
        candidateStartNodesBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateStartNodes), true);

    CLBuffer candidateRelationshipCounts = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, candidateStartNodes.length);


    int[] globalSizes = new int[]{(int) candidateStartNodes.length};

    CLEvent countRelationshipCandidatesEvent = this.queryKernels.countCandidateRelationshipsKernel.count_candidate_relationships(
        this.queryKernels.queue,
        candidateRelationships.getQueryStartNodeId(),
        candidateRelationships.getQueryEndNodeId(),
        candidateRelationships.getRelationshipType(),
        this.dataBuffers.dataNodeRelationshipsBuffer,
        this.dataBuffers.dataRelationshipTypesBuffer,
        this.dataBuffers.dataRelationshipIndicesBuffer,
        candidateRelationshipCounts,
        candidateStartNodesBuffer,
        this.queryBuffers.candidateIndicatorsBuffer,
        this.dataNodeCount,
        globalSizes,
        null
    );

    candidateRelationships.setCandidateStartNodes(candidateStartNodesBuffer);


    return candidateRelationshipCounts.read(this.queryKernels.queue, countRelationshipCandidatesEvent);
  }
}