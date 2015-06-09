package se.simonevertsson.gpu;

import org.bridj.Pointer;

/**
 * Created by simon on 2015-06-09.
 */
public class EdgeCandidates {
    private final Pointer<Integer> candidateEndNodeIndicies;
    private final Pointer<Integer> candidateEndNodes;
    private final Pointer<Integer> candidateNodes;
    private final int queryStartNodeId;
    private final int queryEndNodeId;

    public EdgeCandidates(int queryStartNodeId, int queryEndNodeId, Pointer<Integer> candidateEndNodeIndicies, Pointer<Integer> candidateEndNodes, Pointer<Integer> candidateNodes) {
        this.queryStartNodeId = queryStartNodeId;
        this.queryEndNodeId = queryEndNodeId;
        this.candidateEndNodeIndicies = candidateEndNodeIndicies;
        this.candidateEndNodes = candidateEndNodes;
        this.candidateNodes = candidateNodes;
    }

    public Pointer<Integer> getCandidateEndNodeIndicies() {
        return candidateEndNodeIndicies;
    }

    public Pointer<Integer> getCandidateEndNodes() {
        return candidateEndNodes;
    }

    public Pointer<Integer> getCandidateNodes() {
        return candidateNodes;
    }

    public int getQueryStartNodeId() {
        return queryStartNodeId;
    }

    public int getQueryEndNodeId() {
        return queryEndNodeId;
    }
}
