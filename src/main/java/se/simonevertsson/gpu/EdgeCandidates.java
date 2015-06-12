package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import org.bridj.Pointer;

/**
 * Created by simon on 2015-06-09.
 */
public class EdgeCandidates {
    private final CLBuffer<Integer> candidateEndNodeIndicies;
    private final CLBuffer<Integer> candidateEndNodes;
    private final CLBuffer<Integer> candidateStartNodes;
    private final int queryStartNodeId;
    private final int queryEndNodeId;
    private final int count;

    public EdgeCandidates(int queryStartNodeId, int queryEndNodeId, CLBuffer<Integer> candidateEndNodeIndicies, CLBuffer<Integer> candidateEndNodes, CLBuffer<Integer> candidateStartNodes) {
        this.queryStartNodeId = queryStartNodeId;
        this.queryEndNodeId = queryEndNodeId;
        this.candidateEndNodeIndicies = candidateEndNodeIndicies;
        this.candidateEndNodes = candidateEndNodes;
        this.candidateStartNodes = candidateStartNodes;
        this.count = (int) this.candidateEndNodeIndicies.getElementCount();
    }

    public CLBuffer<Integer> getCandidateEndNodeIndicies() {
        return candidateEndNodeIndicies;
    }

    public CLBuffer<Integer> getCandidateEndNodes() {
        return candidateEndNodes;
    }

    public CLBuffer<Integer> getCandidateStartNodes() {
        return candidateStartNodes;
    }

    public int getQueryStartNodeId() {
        return queryStartNodeId;
    }

    public int getQueryEndNodeId() {
        return queryEndNodeId;
    }

    public int getCount() {
        return count;
    }
}
