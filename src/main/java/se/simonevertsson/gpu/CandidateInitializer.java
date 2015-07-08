package se.simonevertsson.gpu;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.util.ArrayList;

public class CandidateInitializer {
    private final QueryContext queryContext;
    private final QueryBuffers queryBuffers;
    private final CandidateChecker candidateChecker;
    private final CandidateExplorer candidateExplorer;


    public CandidateInitializer(QueryContext queryContext, QueryKernels queryKernels, BufferContainer bufferContainer) {
        this.queryContext = queryContext;
        this.queryBuffers = bufferContainer.queryBuffers;
        this.candidateChecker = new CandidateChecker(queryKernels, bufferContainer, this.queryContext.dataNodeCount);
        this.candidateExplorer = new CandidateExplorer(queryKernels, bufferContainer, this.queryContext);
    }

    void candidateInitialization(ArrayList<Node> visitOrder) throws IOException {
        boolean initializedQueryNodes[] = new boolean[this.queryContext.queryNodeCount];
        for (Node queryNode : visitOrder) {
            if (!initializedQueryNodes[((int) queryNode.getId())]) {
                this.candidateChecker.checkCandidates(this.queryContext.gpuQuery, queryNode);
//                System.out.println("Candidate indicators after candidate checking of query node " + queryNode.getId());
//                QueryUtils.printCandidateIndicatorMatrix(this.queryBuffers.candidateIndicatorsPointer, queryContext.dataNodeCount);
            }

            int queryNodeRelationshipStartIndex = this.queryContext.gpuQuery.getRelationshipIndices()[((int) queryNode.getId())];
            if(this.queryContext.gpuQuery.getNodeRelationships()[queryNodeRelationshipStartIndex] != -1) {

                /* The current query node has relationships, hence we explore the neighborhood */

                int[] candidateArray = QueryUtils.gatherCandidateArray(
                        this.queryBuffers.candidateIndicatorsPointer,
                        this.queryContext.dataNodeCount,
                        (int) queryNode.getId());

                if (candidateArray.length > 0) {
                    this.candidateExplorer.exploreCandidates((int) queryNode.getId(), candidateArray);

                    /* We have explored the neighborhood, mark query nodes related to the current query node as initialized */
                    for (Relationship adjacency : queryNode.getRelationships()) {
                        initializedQueryNodes[((int) adjacency.getEndNode().getId())] = true;
                    }
//                    System.out.println("Candidate indicators after candidate neighborhood exploration of query node " + queryNode.getId());
//                    QueryUtils.printCandidateIndicatorMatrix(this.queryBuffers.candidateIndicatorsPointer, queryContext.dataNodeCount);
                } else {
                    throw new IllegalStateException("No candidates for query node " + queryNode.getId() + " were found.");
                }
            }

            initializedQueryNodes[((int) queryNode.getId())] = true;


        }
    }
}