package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Relationship;

/**
 * Created by simon on 2015-06-09.
 */
public class RelationshipCandidates {


    private final Relationship relationship;
    private CLBuffer<Integer> candidateEndNodeIndicies;
    private CLBuffer<Integer> candidateEndNodes;
    private CLBuffer<Integer> candidateStartNodes;
    private int totalCount;
    private int startNodeCount;


    public RelationshipCandidates(Relationship relationship) {
        this.relationship = relationship;
    }

    public CLBuffer<Integer> getCandidateEndNodeIndices() {
        return candidateEndNodeIndicies;
    }

    public CLBuffer<Integer> getCandidateEndNodes() {
        return candidateEndNodes;
    }

    public CLBuffer<Integer> getCandidateStartNodes() {
        return candidateStartNodes;
    }

    public void setCandidateStartNodes(CLBuffer<Integer> candidateStartNodes) {
        this.candidateStartNodes = candidateStartNodes;
        this.startNodeCount = (int) this.candidateStartNodes.getElementCount();
    }

    public void setCandidateEndNodeIndicies(CLBuffer<Integer> candidateEndNodeIndicies) {
        this.candidateEndNodeIndicies = candidateEndNodeIndicies;
        this.totalCount = (int) this.candidateEndNodeIndicies.getElementCount()-1;
    }

    public void setCandidateEndNodes(CLBuffer<Integer> candidateEndNodes) {
        this.candidateEndNodes = candidateEndNodes;
    }

    public int getQueryStartNodeId() {
        return (int) this.relationship.getStartNode().getId();
    }

    public int getQueryEndNodeId() {
        return (int) this.relationship.getEndNode().getId();
    }

    public int getTotalCount() {
        return totalCount;
    }

    public int getStartNodeCount() {
        return startNodeCount;
    }

    public Relationship getRelationship() {
        return this.relationship;
    }
}
