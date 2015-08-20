package se.simonevertsson.gpu;

import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by simon on 2015-05-12.
 */
public class GpuGraphConverter {

    private List<Node> nodes;
    private LabelDictionary labelDictionary;
    private TypeDictionary typeDictionary;
    private QueryIdDictionary nodeIdDictionary;

    private ArrayList<Integer> labelIndices;
    private ArrayList<Integer> nodeLabels;
    private ArrayList<Integer> relationshipIndices;
    private ArrayList<Integer> nodeRelationships;
    private ArrayList<Integer> relationshipTypes;
    private QueryIdDictionary relationshipIdDictionary;

    public GpuGraphConverter(List<Node> nodes, LabelDictionary labelDictionary, TypeDictionary typeDictionary) {
        this.nodes = nodes;
        this.labelDictionary = labelDictionary;
        this.typeDictionary = typeDictionary;
    }

    public GpuGraph convert() {
        initializeLists();
        for(Node node : this.nodes) {
            nodeIdDictionary.add(node.getId());
        }

        int currentLabelIndex = 0;
        int currentRelationshipIndex = 0;
        for(Node startNode : this.nodes) {
            currentLabelIndex = addLabels(currentLabelIndex, startNode);
            currentRelationshipIndex = addRelationships(currentRelationshipIndex, startNode);
        }

        appendLastIndexIfNotEmpty(labelIndices, currentLabelIndex);
        appendLastIndexIfNotEmpty(relationshipIndices, currentRelationshipIndex);

        /*
            If no node has labels the label array will be empty. Empty arrays are not supported by JavaCL hence a
            dummy element is added in this case.
         */
        if(nodeLabels.isEmpty()) {
            nodeLabels.add(-1);
        }

        return new GpuGraph(labelIndices, nodeLabels, relationshipIndices, nodeRelationships, relationshipTypes, nodeIdDictionary, relationshipIdDictionary);
    }

    private int addRelationships(int currentRelationshipIndex, Node sourceNode) {
        Iterable<Relationship> nodeRelationships = sourceNode.getRelationships(Direction.OUTGOING);
        int relationshipCount = addRelationships(nodeRelationships);

        relationshipIndices.add(currentRelationshipIndex);
        currentRelationshipIndex += relationshipCount;
        return currentRelationshipIndex;
    }

    private int addLabels(int currentLabelIndex, Node sourceNode) {
        Iterable<Label> labels = sourceNode.getLabels();
        int labelCount = convertAndAddLabels(labels);

        labelIndices.add(currentLabelIndex);
        currentLabelIndex += labelCount;
        return currentLabelIndex;
    }

    private void appendLastIndexIfNotEmpty(ArrayList<Integer> indexList, int lastIndex) {
        if(!indexList.isEmpty()) {
            indexList.add(lastIndex);
        }
    }

    private int addRelationships(Iterable<Relationship> nodeRelationships) {
        int relationshipCount = 0;
        for(Relationship nodeRelationship : nodeRelationships) {
            this.relationshipIdDictionary.add(nodeRelationship.getId());
            int endNodeQueryId = this.nodeIdDictionary.getQueryId(nodeRelationship.getEndNode().getId());
            this.nodeRelationships.add(endNodeQueryId);
            if(nodeRelationship.getType() != null) {
                int relationshipTypeId = this.typeDictionary.insertType(nodeRelationship.getType().name());
                this.relationshipTypes.add(relationshipTypeId);
            } else {
                this.relationshipTypes.add(-1);
            }
            relationshipCount++;
        }

//        if(relationshipCount == 0) {
//            this.nodeRelationships.add(-1);
//            this.relationshipTypes.add(-1);
//            relationshipCount++;
//        }

        return relationshipCount;
    }

    private int convertAndAddLabels(Iterable<Label> labels) {
        int labelCount = 0;
        for(Label nodeLabel : labels) {
            int nodeLabelId = this.labelDictionary.insertLabel(nodeLabel.name());
            this.nodeLabels.add(nodeLabelId);
            labelCount++;
        }

//        if(labelCount == 0) {
//            this.nodeLabels.add(-1);
//            labelCount++;
//        }

        return labelCount;
    }

    private void initializeLists() {
        this.nodeIdDictionary = new QueryIdDictionary();
        this.relationshipIdDictionary = new QueryIdDictionary();
        this.labelIndices = new ArrayList<Integer>();
        this.nodeLabels = new ArrayList<Integer>();
        this.relationshipIndices = new ArrayList<Integer>();
        this.nodeRelationships = new ArrayList<Integer>();
        this.relationshipTypes = new ArrayList<Integer>();
    }
}
