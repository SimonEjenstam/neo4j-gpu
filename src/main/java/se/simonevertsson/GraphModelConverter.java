package se.simonevertsson;

import com.sun.corba.se.impl.orbutil.graph.Graph;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.Iterator;

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

        return new GpuGraphModel(labelIndicies, nodeLabels, adjecenyIndicies, nodeAdjecencies);
    }

    public static GpuGraphModel convertNodesToGpuGraphModel(QueryGraph queryGraph, LabelDictionary labelDictionary) {
        ArrayList<Integer> labelIndicies = new ArrayList<Integer>();
        ArrayList<Integer> nodeLabels = new ArrayList<Integer>();
        ArrayList<Integer> adjecenyIndicies = new ArrayList<Integer>();
        ArrayList<Long> nodeAdjecencies = new ArrayList<Long>();

        int currentAdjacencyIndex = 0;
        int currentLabelIndex = 0;
        for(Node sourceNode : queryGraph.nodes) {
            Iterable<Label> labels = sourceNode.getLabels();
            int labelCount = 0;
            for(Label nodeLabel : labels) {
                int nodeLabelId = labelDictionary.insertLabel(nodeLabel.name());
                nodeLabels.add(nodeLabelId);
                labelCount++;
            }
            labelIndicies.add(currentLabelIndex);
            currentLabelIndex += labelCount;

            int relationshipCount = 0;
            for(Relationship nodeRelationship : queryGraph.relationships) {
                if(nodeRelationship.getStartNode().getId() == sourceNode.getId()) {
                    // Add to adjacency list
                    nodeAdjecencies.add(nodeRelationship.getEndNode().getId());
                    relationshipCount++;
                }
            }
            adjecenyIndicies.add(currentAdjacencyIndex);
            currentAdjacencyIndex += relationshipCount;
        }

        return new GpuGraphModel(labelIndicies, nodeLabels, adjecenyIndicies, nodeAdjecencies);
    }
}
