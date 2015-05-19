package se.simonevertsson.gpu;

import org.neo4j.graphdb.*;

import java.util.ArrayList;

/**
 * Created by simon on 2015-05-12.
 */
public class GraphModelConverter {

    public static GpuGraphModel convertNodesToGpuGraphModel(Iterable<Node> nodes, LabelDictionary labelDictionary) {
        ArrayList<Integer> labelIndicies = new ArrayList<Integer>();
        ArrayList<Integer> nodeLabels = new ArrayList<Integer>();
        ArrayList<Integer> adjecenyIndicies = new ArrayList<Integer>();
        ArrayList<Long> nodeAdjecencies = new ArrayList<Long>();

        int currentAdjacencyIndex = 0;
        int currentLabelIndex = 0;
        for(Node sourceNode : nodes) {
            Iterable<Label> labels = sourceNode.getLabels();
            int labelCount = 0;
            for(Label nodeLabel : labels) {
                int nodeLabelId = labelDictionary.insertLabel(nodeLabel.name());
                nodeLabels.add(nodeLabelId);
                labelCount++;
            }
            labelIndicies.add(currentLabelIndex);
            currentLabelIndex += labelCount;

            Iterable<Relationship> nodeRelationships = sourceNode.getRelationships(Direction.OUTGOING);
            int relationshipCount = 0;
            for(Relationship nodeRelationship : nodeRelationships) {
                nodeAdjecencies.add(nodeRelationship.getEndNode().getId());
                relationshipCount++;
            }
            adjecenyIndicies.add(currentAdjacencyIndex);
            currentAdjacencyIndex += relationshipCount;
        }

        if(!adjecenyIndicies.isEmpty()) {
            adjecenyIndicies.add(currentAdjacencyIndex);
        }

        if(!labelIndicies.isEmpty()) {
            labelIndicies.add(currentLabelIndex);
        }

        return new GpuGraphModel(labelIndicies, nodeLabels, adjecenyIndicies, nodeAdjecencies);
    }
}
