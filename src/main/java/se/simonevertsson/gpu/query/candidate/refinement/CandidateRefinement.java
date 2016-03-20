package se.simonevertsson.gpu.query.candidate.refinement;

import org.neo4j.graphdb.Node;
import se.simonevertsson.gpu.query.QueryContext;
import se.simonevertsson.gpu.query.QueryUtils;
import se.simonevertsson.gpu.buffer.BufferContainer;
import se.simonevertsson.gpu.buffer.QueryBuffers;
import se.simonevertsson.gpu.kernel.QueryKernels;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CandidateRefinement {

  private final QueryBuffers queryBuffers;
  private final int queryNodeCount;
  private final int dataNodeCount;
  private final CandidateRefinery candidateRefinery;
  private final QueryContext queryContext;

  public CandidateRefinement(QueryContext queryContext, QueryKernels queryKernels, BufferContainer bufferContainer) {
    this.queryContext = queryContext;
    this.queryBuffers = bufferContainer.queryBuffers;
    this.queryNodeCount = queryContext.queryNodeCount;
    this.dataNodeCount = queryContext.dataNodeCount;
    this.candidateRefinery = new CandidateRefinery(queryKernels, bufferContainer, queryContext);
  }

  public void refine(List<Node> visitOrder) throws IOException {
    boolean[] oldCandidateIndicators = QueryUtils.pointerBooleanToArray(this.queryBuffers.candidateIndicatorsPointer, this.dataNodeCount * this.queryNodeCount);
    boolean candidateIndicatorsHasChanged = true;
    while (candidateIndicatorsHasChanged) {
      System.out.println("Refining candidates.");
      for (Node queryNode : visitOrder) {
        int queryNodeId = this.queryContext.gpuQuery.getNodeIdDictionary().getQueryId(queryNode.getId());
        int[] candidateArray = QueryUtils.gatherCandidateArray(this.queryBuffers.candidateIndicatorsPointer, this.dataNodeCount, queryNodeId);
        if (candidateArray.length > 0) {
          this.candidateRefinery.refineCandidates(queryNodeId, candidateArray);
        } else {
          throw new IllegalStateException("Candidate refinement yielded no candidates for runner node " + queryNodeId);
        }
      }

      boolean[] newCandidateIndicators = QueryUtils.pointerBooleanToArray(this.queryBuffers.candidateIndicatorsPointer, this.dataNodeCount * this.queryNodeCount);

      candidateIndicatorsHasChanged = !Arrays.equals(oldCandidateIndicators, newCandidateIndicators);
      oldCandidateIndicators = newCandidateIndicators;
    }
  }
}