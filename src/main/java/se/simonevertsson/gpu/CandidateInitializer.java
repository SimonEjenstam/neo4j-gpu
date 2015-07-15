package se.simonevertsson.gpu;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.util.ArrayList;
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

    void candidateInitialization(List<Node> visitOrder) throws IOException {
        boolean initializedQueryNodes[] = new boolean[this.queryContext.queryNodeCount];
        for (Node queryNode : visitOrder) {
            int queryNodeId = this.queryContext.gpuQuery.getQueryIdDictionary().getQueryId(queryNode.getId());

            if (!initializedQueryNodes[queryNodeId]) {
                this.candidateChecker.checkCandidates(this.queryContext.gpuQuery, queryNode);
            }

            int queryNodeRelationshipStartIndex = this.queryContext.gpuQuery.getRelationshipIndices()[queryNodeId];
            if(this.queryContext.gpuQuery.getNodeRelationships()[queryNodeRelationshipStartIndex] != -1) {

                /* The current query node has relationships, hence we explore the neighborhood */

                int[] candidateArray = QueryUtils.gatherCandidateArray(
                        this.queryBuffers.candidateIndicatorsPointer,
                        this.queryContext.dataNodeCount,
                        queryNodeId);

                if (candidateArray.length > 0) {
                    this.candidateExplorer.exploreCandidates(queryNodeId, candidateArray);

                    /* We have explored the neighborhood, mark query nodes related to the current query node as initialized */
                    for (Relationship neighborhoodRelationship : queryNode.getRelationships()) {
                        int neighborQueryNodeId = this.queryContext.gpuQuery.getQueryIdDictionary()
                                .getQueryId(neighborhoodRelationship.getEndNode().getId());
                        initializedQueryNodes[neighborQueryNodeId] = true;
                    }
//                    System.out.println("Candidate indicators after candidate neighborhood exploration of query node " + queryNode.getId());
//                    QueryUtils.printCandidateIndicatorMatrix(this.queryBuffers.candidateIndicatorsPointer, queryContext.dataNodeCount);
                } else {
                    throw new IllegalStateException("No candidates for query node " + queryNodeId + " were found.");
                }
            }

            initializedQueryNodes[queryNodeId] = true;


        }
    }
}