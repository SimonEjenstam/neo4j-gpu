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

    public static QueryGraph generateQueryGraph(QueryGraph queryGraph, LabelDictionary labelDictionary) {
        Relationship initialRelationShip = findInitialRelationship(queryGraph.relationships, labelDictionary);

        HashMap<Long, Boolean> visitedNodes = new HashMap<Long, Boolean>();

        queryGraph.visitOrder.add(initialRelationShip.getStartNode());
        visitedNodes.put(initialRelationShip.getStartNode().getId(), true);

        long originalStartId = initialRelationShip.getStartNode().getId();
        for (Iterator<Relationship> iter = queryGraph.relationships.listIterator(); iter.hasNext(); ) {
            Relationship relationship = iter.next();
            long currentStartId = relationship.getStartNode().getId();
            long currentEndId = relationship.getEndNode().getId();
            if(currentStartId == originalStartId && !visitedNodes.containsKey(currentEndId)) {
                queryGraph.spanningTree.add(relationship);
                queryGraph.visitOrder.add(relationship.getEndNode());
                visitedNodes.put(relationship.getEndNode().getId(), true);
                iter.remove();
            }
        }

        while(queryGraph.visitOrder.size() < queryGraph.nodes.size()) {
            for (Iterator<Relationship> iter = queryGraph.relationships.listIterator(); iter.hasNext(); ) {
                Relationship relationship = iter.next();
                long currentStartId = relationship.getStartNode().getId();
                long currentEndId = relationship.getEndNode().getId();
                if(visitedNodes.containsKey(currentStartId) && !visitedNodes.containsKey(currentEndId)) {
                    queryGraph.spanningTree.add(relationship);
                    queryGraph.visitOrder.add(relationship.getEndNode());
                    visitedNodes.put(relationship.getEndNode().getId(), true);
                    iter.remove();
                } else if(!visitedNodes.containsKey(currentStartId) && visitedNodes.containsKey(currentEndId)) {
                    queryGraph.spanningTree.add(relationship);
                    queryGraph.visitOrder.add(relationship.getStartNode());
                    visitedNodes.put(relationship.getStartNode().getId(), true);
                } else if(visitedNodes.containsKey(currentStartId) && visitedNodes.containsKey(currentEndId)) {
                    iter.remove();
                }
            }
        }
        return queryGraph;
    }

    private static Relationship findInitialRelationship(ArrayList<Relationship> queryGraphRelationships, LabelDictionary labelDictionary) {
        HashMap<String, Integer> labelCounter = countLabels(queryGraphRelationships, labelDictionary);
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

    private static HashMap<String, Integer> countLabels(ArrayList<Relationship> queryGraphRelationships, LabelDictionary labelDictionary) {
        HashMap<String, Integer> labelCounter = new HashMap<String, Integer>();
        HashSet<Long> visitedNodes = new HashSet<Long>();
        for(Relationship relationship : queryGraphRelationships) {
            Node startNode = relationship.getStartNode();
            Node endNode = relationship.getEndNode();
            addNodeLabelsToCount(labelCounter, visitedNodes, startNode, labelDictionary);
            addNodeLabelsToCount(labelCounter, visitedNodes, endNode, labelDictionary);
        }

        return labelCounter;
    }

    private static void addNodeLabelsToCount(HashMap<String, Integer> labelCounter, HashSet<Long> visitedNodes, Node startNode, LabelDictionary labelDictionary) {
        if( !visitedNodes.contains(startNode.getId()) ) {
            for (Label label : startNode.getLabels()) {
                labelDictionary.insertLabel(label.name());
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
        int nodeDegree = node.getDegree(Direction.OUTGOING);
        return nodeDegree / (float) labelFrequency;
    }
}
