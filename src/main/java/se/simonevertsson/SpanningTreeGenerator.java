package se.simonevertsson;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by simon on 2015-05-12.
 */
public class SpanningTreeGenerator {

    public static QueryGraph generateQueryGraph(QueryGraph queryGraph) {
        Relationship initialRelationShip = findInitialRelationship(queryGraph.relationships);
        queryGraph.visitOrder.add(initialRelationShip.getStartNode().getId());

        HashMap<Long, Boolean> visitedNodes = new HashMap<Long, Boolean>();

        long originalStartId = initialRelationShip.getStartNode().getId();
        for (Iterator<Relationship> iter = queryGraph.relationships.listIterator(); iter.hasNext(); ) {
            Relationship relationship = iter.next();
            long currentStartId = relationship.getStartNode().getId();
            long currentEndId = relationship.getEndNode().getId();
            if(currentStartId == originalStartId || currentEndId == originalStartId) {
                queryGraph.spanningTree.add(relationship.getId());
                visitedNodes.put(relationship.getStartNode().getId(), true);
                visitedNodes.put(relationship.getEndNode().getId(), true);
            }
        }

        while(visitedNodes.keySet().size() < queryGraph.nodes.size()) {
            for (Iterator<Relationship> iter = queryGraph.relationships.listIterator(); iter.hasNext(); ) {
                Relationship relationship = iter.next();
                long currentStartId = relationship.getStartNode().getId();
                long currentEndId = relationship.getEndNode().getId();
                if (visitedNodes.containsKey(currentStartId)) {
                    queryGraph.spanningTree.add(relationship.getId());
                    visitedNodes.put(relationship.getEndNode().getId(), true);
                    iter.remove();
                }
            }
        }
        return queryGraph;
    }

    private static Relationship findInitialRelationship(ArrayList<Relationship> queryGraphRelationships) {
        HashMap<String, Integer> labelCounter = countLabels(queryGraphRelationships);
        float maxEstimateSum = 0;
        Relationship initialRelationship = null;

        for(Relationship relationship : queryGraphRelationships) {
            float startEstimate = calculateNodeRankEstimate(labelCounter, relationship.getStartNode());
            float endEstimate = calculateNodeRankEstimate(labelCounter, relationship.getEndNode());
            if(startEstimate > endEstimate && (startEstimate + endEstimate) >= maxEstimateSum) {
                initialRelationship = relationship;
                maxEstimateSum = (startEstimate + endEstimate);
            }
        }

        return initialRelationship;
    }

    private static HashMap<String, Integer> countLabels(ArrayList<Relationship> queryGraphRelationships) {
        HashMap<String, Integer> labelCounter = new HashMap<String, Integer>();
        HashSet<Long> visitedNodes = new HashSet<Long>();
        for(Relationship relationship : queryGraphRelationships) {
            Node startNode = relationship.getStartNode();
            Node endNode = relationship.getEndNode();
            addNodeLabelsToCount(labelCounter, visitedNodes, startNode);
            addNodeLabelsToCount(labelCounter, visitedNodes, endNode);
        }
        return labelCounter;
    }

    private static void addNodeLabelsToCount(HashMap<String, Integer> labelCounter, HashSet<Long> visitedNodes, Node startNode) {
        if( !visitedNodes.contains(startNode.getId()) ) {
            for (Label label : startNode.getLabels()) {

                Integer currentLabelCountObject = labelCounter.get(label.name());
                int currentLabelCount = currentLabelCountObject == null ? 0 : currentLabelCountObject;
                labelCounter.put(label.name(), currentLabelCount + 1);
            }
            visitedNodes.add(startNode.getId());
        }
    }


    private static float calculateNodeRankEstimate(HashMap<String, Integer> labelCounter, Node node) {
        int labelFrequency = 0;
        for(Label label : node.getLabels()) {
            labelFrequency += labelCounter.get(label.name());
        }
        int nodeDegree = node.getDegree(Direction.BOTH);
        return nodeDegree / (float) labelFrequency;
    }
}
