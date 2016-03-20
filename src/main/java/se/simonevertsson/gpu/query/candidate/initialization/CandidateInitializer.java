package se.simonevertsson.gpu.query.candidate.initialization;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.gpu.query.QueryContext;
import se.simonevertsson.gpu.query.QueryUtils;
import se.simonevertsson.gpu.buffer.BufferContainer;
import se.simonevertsson.gpu.buffer.QueryBuffers;
import se.simonevertsson.gpu.kernel.QueryKernels;

import java.io.IOException;
import java.util.List;

public class CandidateInitializer {
  private final QueryContext queryContext;
  private final QueryBuffers queryBuffers;
  private final CandidateChecker candidateChecker;
  private final CandidateExplorer candidateExplorer;


  public CandidateInitializer(QueryContext queryContext, QueryKernels queryKernels, BufferContainer bufferContainer) {
    this.queryContext = queryContext;
    this.queryBuffers = bufferContainer.queryBuffers;
    this.candidateChecker = new CandidateChecker(queryContext, queryKernels, bufferContainer, this.queryContext.dataNodeCount);
    this.candidateExplorer = new CandidateExplorer(queryKernels, bufferContainer, this.queryContext);
  }

  public void candidateInitialization(List<Node> visitOrder) throws IOException {
    boolean initializedQueryNodes[] = new boolean[this.queryContext.queryNodeCount];
    for (Node queryNode : visitOrder) {

      int queryNodeId = this.queryContext.gpuQuery.getNodeIdDictionary().getQueryId(queryNode.getId());

      if (!initializedQueryNodes[queryNodeId]) {
        this.candidateChecker.checkCandidates(this.queryContext.gpuQuery, queryNode);
      }

      int queryNodeRelationshipStartIndex = this.queryContext.gpuQuery.getRelationshipIndices()[queryNodeId];
      int queryNodeRelationshipEndIndex = this.queryContext.gpuQuery.getRelationshipIndices()[queryNodeId + 1];
      if (queryNodeRelationshipStartIndex != queryNodeRelationshipEndIndex) {

        // The current runner node has relationships, hence we explore the neighborhood
        int[] candidateArray = QueryUtils.gatherCandidateArray(
            this.queryBuffers.candidateIndicatorsPointer,
            this.queryContext.dataNodeCount,
            queryNodeId);

        if (candidateArray.length > 0) {
          this.candidateExplorer.exploreCandidates(queryNodeId, candidateArray);

          // We have explored the neighborhood, mark runner nodes related to the current runner node as initialized
          for (Relationship neighborhoodRelationship : queryNode.getRelationships()) {
            int neighborQueryNodeId = this.queryContext.gpuQuery.getNodeIdDictionary()
                .getQueryId(neighborhoodRelationship.getEndNode().getId());
            ;
            initializedQueryNodes[neighborQueryNodeId] = true;
          }
        } else {
          throw new IllegalStateException("No candidates for runner node " + queryNodeId + " were found.");
        }
      }

      initializedQueryNodes[queryNodeId] = true;


    }
  }
}