package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import org.neo4j.graphdb.Relationship;

import java.util.Arrays;

/**
 * Created by simon on 2015-06-09.
 */
public class CandidateRelationships {


    private final Relationship relationship;
    private final QueryKernels queryKernels;
    private final QueryIdDictionary queryIdDictionary;
    private CLBuffer<Integer> candidateEndNodeIndices;
    private CLBuffer<Integer> candidateEndNodes;
    private CLBuffer<Integer> candidateStartNodes;
    private int endNodeCount;
    private int startNodeCount;


    public CandidateRelationships(Relationship relationship, QueryIdDictionary queryIdDictionary, QueryKernels queryKernels) {
        this.relationship = relationship;
        this.queryIdDictionary = queryIdDictionary;
        this.queryKernels = queryKernels;
    }

    public CLBuffer<Integer> getCandidateEndNodeIndices() {
        return candidateEndNodeIndices;
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

    public void setCandidateEndNodeIndices(CLBuffer<Integer> candidateEndNodeIndices) {
        this.candidateEndNodeIndices = candidateEndNodeIndices;
    }

    public void setCandidateEndNodes(CLBuffer<Integer> candidateEndNodes) {
        this.candidateEndNodes = candidateEndNodes;
    }

    public int getQueryStartNodeId() {
        return this.queryIdDictionary.getQueryId(this.relationship.getStartNode().getId());
    }

    public int getQueryEndNodeId() {
        return this.queryIdDictionary.getQueryId(this.relationship.getEndNode().getId());
    }

    public int getEndNodeCount() {
        return endNodeCount;
    }

    public int getStartNodeCount() {
        return startNodeCount;
    }

    public Relationship getRelationship() {
        return this.relationship;
    }

    public void setEndNodeCount(int endNodeCount) {
        this.endNodeCount = endNodeCount;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Candidate relationships for query relationship ");
        builder.append(this.relationship.getId());
        builder.append(" ( ");
        builder.append(this.queryIdDictionary.getQueryId(this.relationship.getStartNode().getId()));
        builder.append(" --> ");
        builder.append(this.queryIdDictionary.getQueryId(this.relationship.getEndNode().getId()));
        builder.append(" )\n");

        builder.append("Start nodes: ");
        builder.append(Arrays.toString(QueryUtils.pointerIntegerToArray(this.candidateStartNodes.read(this.queryKernels.queue), startNodeCount)));
        builder.append('\n');

        builder.append("End node indices: ");
        builder.append(Arrays.toString(QueryUtils.pointerIntegerToArray(this.candidateEndNodeIndices.read(this.queryKernels.queue), this.startNodeCount + 1)));
        builder.append('\n');

        builder.append("End nodes: ");
        builder.append(Arrays.toString(QueryUtils.pointerIntegerToArray(this.candidateEndNodes.read(this.queryKernels.queue), this.endNodeCount)));
        builder.append('\n');

        return builder.toString();
    }


}
