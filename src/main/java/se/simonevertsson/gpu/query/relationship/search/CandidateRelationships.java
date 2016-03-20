package se.simonevertsson.gpu.query.relationship.search;

import com.nativelibs4java.opencl.CLBuffer;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.gpu.query.QueryContext;
import se.simonevertsson.gpu.dictionary.QueryIdDictionary;
import se.simonevertsson.gpu.query.QueryUtils;
import se.simonevertsson.gpu.dictionary.TypeDictionary;
import se.simonevertsson.gpu.kernel.QueryKernels;

import java.util.Arrays;

/**
 * Created by simon on 2015-06-09.
 */
public class CandidateRelationships {


    private final Relationship relationship;
    private final QueryKernels queryKernels;
    private final QueryIdDictionary nodeIdDictionary;
    private final TypeDictionary typeDictionary;
    private CLBuffer<Integer> candidateEndNodeIndices;
    private CLBuffer<Integer> candidateEndNodes;
    private CLBuffer<Integer> candidateStartNodes;
    private int endNodeCount;
    private int startNodeCount;
    private CLBuffer<Integer> candidateRelationshipIndices;


    public CandidateRelationships(Relationship relationship, QueryContext queryContext, QueryKernels queryKernels) {
        this.relationship = relationship;
        this.nodeIdDictionary = queryContext.gpuQuery.getNodeIdDictionary();
        this.typeDictionary = queryContext.typeDictionary;
        this.queryKernels = queryKernels;
    }



    public int getRelationshipType() {

        return this.relationship.getType() != null ? this.typeDictionary.getIdForType(this.relationship.getType().name()) : -1;
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
        return this.nodeIdDictionary.getQueryId(this.relationship.getStartNode().getId());
    }

    public int getQueryEndNodeId() {
        return this.nodeIdDictionary.getQueryId(this.relationship.getEndNode().getId());
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
        builder.append("Candidate relationships for runner relationship ");
        builder.append(this.relationship.getId());
        builder.append(" ( ");
        builder.append(this.nodeIdDictionary.getQueryId(this.relationship.getStartNode().getId()));
        builder.append(" --> ");
        builder.append(this.nodeIdDictionary.getQueryId(this.relationship.getEndNode().getId()));
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

        builder.append("Relationship indices: ");
        builder.append(Arrays.toString(QueryUtils.pointerIntegerToArray(this.candidateRelationshipIndices.read(this.queryKernels.queue), this.endNodeCount)));
        builder.append('\n');

        return builder.toString();
    }


    public void setCandidateRelationshipIndices(CLBuffer<Integer> candidateRelationshipIndices) {
        this.candidateRelationshipIndices = candidateRelationshipIndices;
    }

    public CLBuffer<Integer> getRelationshipIndices() {
        return this.candidateRelationshipIndices;
    }

    public void release() {
        this.candidateStartNodes.release();
        this.candidateEndNodeIndices.release();
        this.candidateEndNodes.release();
        this.candidateRelationshipIndices.release();
    }
}
