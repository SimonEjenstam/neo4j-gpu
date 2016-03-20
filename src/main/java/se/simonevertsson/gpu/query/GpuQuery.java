package se.simonevertsson.gpu.query;

import org.neo4j.graphdb.Node;
import se.simonevertsson.gpu.buffer.BufferContainer;
import se.simonevertsson.gpu.buffer.BufferContainerGenerator;
import se.simonevertsson.gpu.kernel.QueryKernels;
import se.simonevertsson.gpu.query.candidate.initialization.CandidateInitializer;
import se.simonevertsson.gpu.query.relationship.join.CandidateRelationshipJoiner;
import se.simonevertsson.gpu.query.relationship.join.PossibleSolutions;
import se.simonevertsson.gpu.query.relationship.search.CandidateRelationshipSearcher;
import se.simonevertsson.gpu.query.relationship.search.CandidateRelationships;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * This class excutes graph queries on the GPU using the given {@link QueryContext}.
 */

public class GpuQuery {
  private QueryContext queryContext;
  private final QueryKernels queryKernels;
  private BufferContainer bufferContainer;

  public GpuQuery(QueryContext queryContext) throws IOException {
    this.queryContext = queryContext;
    this.queryKernels = new QueryKernels();
    this.bufferContainer = BufferContainerGenerator.generateBufferContainer(this.queryContext, this.queryKernels);
  }

  public GpuQuery(QueryContext queryContext, QueryKernels queryKernels) throws IOException {
    this.queryContext = queryContext;
    this.queryKernels = queryKernels;
    this.bufferContainer = BufferContainerGenerator.generateBufferContainer(this.queryContext, this.queryKernels);
  }

  public List<QuerySolution> executeQuery(List<Node> visitOrder) throws IOException {
    /****** Candidate initialization step ******/
    CandidateInitializer candidateInitializer =
        new CandidateInitializer(this.queryContext, this.queryKernels, this.bufferContainer);
    candidateInitializer.candidateInitialization(visitOrder);

    /****** Candidate refinement step (Possibly not necessary) ******/
//    CandidateRefinement candidateRefinement =
//            new CandidateRefinement(this.queryContext, this.queryKernels, this.bufferContainer);
//    candidateRefinement.refine(visitOrder);

    /****** Candidate relationship searching step ******/
    CandidateRelationshipSearcher candidateRelationshipSearcher =
        new CandidateRelationshipSearcher(this.queryContext, this.queryKernels, this.bufferContainer);
    HashMap<Integer, CandidateRelationships> relationshipCandidatesHashMap = candidateRelationshipSearcher.searchCandidateRelationships();

    // Release buffers which we don't need anymore
    this.bufferContainer.queryBuffers.candidateIndicatorsBuffer.release();
    this.bufferContainer.queryBuffers.candidateIndicatorsPointer.release();
    this.bufferContainer.dataBuffers.dataLabelIndicesBuffer.release();
    this.bufferContainer.dataBuffers.dataLabelsBuffer.release();
    this.bufferContainer.dataBuffers.dataRelationshipIndicesBuffer.release();
    this.bufferContainer.dataBuffers.dataNodeRelationshipsBuffer.release();
    this.bufferContainer.dataBuffers.dataRelationshipTypesBuffer.release();

    /****** Candidate relationship joining step ******/
    CandidateRelationshipJoiner candidateRelationshipJoiner =
        new CandidateRelationshipJoiner(this.queryContext, this.queryKernels, this.bufferContainer);
    PossibleSolutions solutions = candidateRelationshipJoiner.joinCandidateRelationships(relationshipCandidatesHashMap);

    for (int key : relationshipCandidatesHashMap.keySet()) {
      relationshipCandidatesHashMap.get(key).release();
    }

    return QueryUtils.generateQuerySolutions(this.queryKernels, this.queryContext, solutions);
  }
}
