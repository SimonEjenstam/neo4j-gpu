package se.simonevertsson.gpu;

import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by simon on 2015-05-12.
 */
public class GraphModelConverter {

    private final List<Node> nodes;
    private final LabelDictionary labelDictionary;
    private final TypeDictionary typeDictionary;
    private ArrayList<Integer> labelIndices;
    private ArrayList<Integer> nodeLabels;
    private ArrayList<Integer> relationshipIndices;
    private ArrayList<Integer> nodeRelationships;
    private ArrayList<Integer> relationshipTypes;
    private QueryIdDictionary queryIdDictionary;


    public GraphModelConverter(List<Node> nodes, LabelDictionary labelDictionary, TypeDictionary typeDictionary) {
        this.nodes = nodes;
        this.labelDictionary = labelDictionary;
        this.typeDictionary = typeDictionary;
        this.queryIdDictionary = new QueryIdDictionary();
    }

    public GpuGraphModel convert() {
        initializeLists();
        for(Node node : nodes) {
            queryIdDictionary.add(node.getId());
        }


        int currentLabelIndex = 0;
        int currentRelationshipIndex = 0;
        for(Node startNode : nodes) {
            currentLabelIndex = addLabels(currentLabelIndex, startNode);
            currentRelationshipIndex = addRelationships(currentRelationshipIndex, startNode);
        }

        appendLastIndexIfNotEmpty(labelIndices, currentLabelIndex);
        appendLastIndexIfNotEmpty(relationshipIndices, currentRelationshipIndex);
        return new GpuGraphModel(labelIndices, nodeLabels, relationshipIndices, nodeRelationships, relationshipTypes, queryIdDictionary);
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
            int endNodeQueryId = this.queryIdDictionary.getQueryId(nodeRelationship.getEndNode().getId());
            this.nodeRelationships.add(endNodeQueryId);
            if(nodeRelationship.getType() != null) {
                int relationshipTypeId = this.typeDictionary.insertType(nodeRelationship.getType().name());
                this.relationshipTypes.add(relationshipTypeId);
            } else {
                this.relationshipTypes.add(-1);
            }
            relationshipCount++;
        }

        if(relationshipCount == 0) {
            this.nodeRelationships.add(-1);
            this.relationshipTypes.add(-1);
            relationshipCount++;
        }

        return relationshipCount;
    }

    private int convertAndAddLabels(Iterable<Label> labels) {
        int labelCount = 0;
        for(Label nodeLabel : labels) {
            int nodeLabelId = this.labelDictionary.insertLabel(nodeLabel.name());
            this.nodeLabels.add(nodeLabelId);
            labelCount++;
        }

        if(labelCount == 0) {
            this.nodeLabels.add(-1);
            labelCount++;
        }

        return labelCount;
    }

    private void initializeLists() {
        this.labelIndices = new ArrayList<Integer>();
        this.nodeLabels = new ArrayList<Integer>();
        this.relationshipIndices = new ArrayList<Integer>();
        this.nodeRelationships = new ArrayList<Integer>();
        this.relationshipTypes = new ArrayList<Integer>();
    }
}
