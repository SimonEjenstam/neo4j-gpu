package se.simonevertsson.gpu;

import org.neo4j.graphdb.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class CandidateRefinement {

    private final QueryBuffers queryBuffers;
    private final int queryNodeCount;
    private final int dataNodeCount;
    private final CandidateRefinery candidateRefinery;

    public CandidateRefinement(QueryContext queryContext, QueryKernels queryKernels, BufferContainer bufferContainer) {
        this.queryBuffers = bufferContainer.queryBuffers;
        this.queryNodeCount = queryContext.queryNodeCount;
        this.dataNodeCount = queryContext.dataNodeCount;
        this.candidateRefinery = new CandidateRefinery(queryKernels, bufferContainer, queryContext);
    }

    public void refine(ArrayList<Node> visitOrder) throws IOException {
        boolean[] oldCandidateIndicators = QueryUtils.pointerToArray(this.queryBuffers.candidateIndicatorsPointer, this.dataNodeCount * this.queryNodeCount);
        boolean candidateIndicatorsHasChanged = true;
        while (candidateIndicatorsHasChanged) {
            for (Node queryNode : visitOrder) {
                int[] candidateArray = QueryUtils.gatherCandidateArray(this.queryBuffers.candidateIndicatorsPointer, this.dataNodeCount, (int) queryNode.getId());
                if (candidateArray.length > 0) {
                    this.candidateRefinery.refineCandidates((int) queryNode.getId(), candidateArray);
                } else {
                    throw new IllegalStateException("Candidate refinement yielded no candidates for query node " + queryNode.getId());
                }
            }

            boolean[] newCandidateIndicators = QueryUtils.pointerToArray(this.queryBuffers.candidateIndicatorsPointer, this.dataNodeCount * this.queryNodeCount);
            candidateIndicatorsHasChanged = !Arrays.equals(oldCandidateIndicators, newCandidateIndicators);
            oldCandidateIndicators = newCandidateIndicators;
        }
    }
}