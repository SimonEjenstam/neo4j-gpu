package se.simonevertsson.gpu.graph;

import org.apache.lucene.util.ArrayUtil;
import se.simonevertsson.gpu.query.dictionary.QueryIdDictionary;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by simon on 2015-05-12.
 */
public class GpuGraph {

    private QueryIdDictionary nodeIdDictionary;

    private QueryIdDictionary relationshipIdDictionary;

    private int[] labelIndices;

    private int[] nodeLabels;

    private int[] relationshipIndices;

    private int[] nodeRelationships;

    private int[] relationshipTypes;

    public GpuGraph(ArrayList<Integer> labelIndices, ArrayList<Integer> nodeLabels, ArrayList<Integer> relationshipIndices, ArrayList<Integer> nodeRelationships, ArrayList<Integer> relationshipTypes, QueryIdDictionary nodeIdDictionary, QueryIdDictionary relationshipIdDictionary) {
        this.labelIndices = ArrayUtil.toIntArray(labelIndices);
        this.nodeLabels = ArrayUtil.toIntArray(nodeLabels);
        this.relationshipIndices = ArrayUtil.toIntArray(relationshipIndices);
        this.nodeRelationships = ArrayUtil.toIntArray(nodeRelationships);
        this.relationshipTypes = ArrayUtil.toIntArray(relationshipTypes);
        this.nodeIdDictionary = nodeIdDictionary;
        this.relationshipIdDictionary = relationshipIdDictionary;
    }

    public int[] getNodeLabels() {
        return nodeLabels;
    }

    public int[] getRelationshipIndices() {
        return relationshipIndices;
    }

    public int[] getNodeRelationships() {
        return nodeRelationships;
    }

    public int[] getLabelIndices() {
        return labelIndices;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Node labels: " + Arrays.toString(this.nodeLabels) + "\n");
        builder.append("Label indicies: " + Arrays.toString(this.labelIndices)+ "\n");
        builder.append("Node relationships: " + Arrays.toString(this.nodeRelationships)+ "\n");
        builder.append("Relationship types: " + Arrays.toString(this.relationshipTypes)+ "\n");
        builder.append("Relationship indicies: " + Arrays.toString(this.relationshipIndices));
        return builder.toString();
    }

    public int[] getRelationshipTypes() {
        return relationshipTypes;
    }

    public QueryIdDictionary getNodeIdDictionary() {
        return nodeIdDictionary;
    }

    public QueryIdDictionary getRelationshipIdDictionary() {
        return relationshipIdDictionary;
    }
}
