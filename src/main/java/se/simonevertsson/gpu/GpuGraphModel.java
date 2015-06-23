package se.simonevertsson.gpu;

import org.apache.lucene.util.ArrayUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by simon on 2015-05-12.
 */
public class GpuGraphModel {

    private int[] labelIndices;

    private int[] nodeLabels;

    private int[] relationshipIndices;

    private int[] nodeRelationships;

    private int[] relationshipTypes;

    public GpuGraphModel(ArrayList<Integer> labelIndices, ArrayList<Integer> nodeLabels, ArrayList<Integer> relationshipIndices, ArrayList<Integer> nodeRelationships, ArrayList<Integer> relationshipTypes) {
        this.labelIndices = ArrayUtil.toIntArray(labelIndices);
        this.nodeLabels = ArrayUtil.toIntArray(nodeLabels);
        this.relationshipIndices = ArrayUtil.toIntArray(relationshipIndices);
        this.nodeRelationships = ArrayUtil.toIntArray(nodeRelationships);
        this.relationshipTypes = ArrayUtil.toIntArray(relationshipTypes);
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
}
