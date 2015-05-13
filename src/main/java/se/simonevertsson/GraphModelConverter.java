package se.simonevertsson;

import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by simon on 2015-05-12.
 */
public class GraphModelConverter {

    public static GpuGraphModel convertNodesToGpuGraphModel(ResourceIterable<Node> allNodes) {
        ArrayList<Integer> nodeLabels = new ArrayList<Integer>();
        ArrayList<Integer> adjecenyIndicies = new ArrayList<Integer>();
        ArrayList<Long> nodeAdjecencies = new ArrayList<Long>();

        int currentAdjacencyIndex = 0;
        for(Node sourceNode : allNodes) {
            Iterable<Label> labels = sourceNode.getLabels();
            Iterator<Label> labelIterator = labels.iterator();
            if(labelIterator.hasNext()) {
                nodeLabels.add(labelIterator.next().hashCode());
            } else {
                nodeLabels.add(-1);
            }
            Iterable<Relationship> nodeRelationships = sourceNode.getRelationships(Direction.OUTGOING);
            int relationshipCount = 0;
            for(Relationship nodeRelationship : nodeRelationships) {
                nodeAdjecencies.add(nodeRelationship.getEndNode().getId());
                relationshipCount++;
            }
            adjecenyIndicies.add(currentAdjacencyIndex);
            currentAdjacencyIndex += relationshipCount;
        }

        return new GpuGraphModel(nodeLabels, adjecenyIndicies, nodeAdjecencies);
    }
}
