package se.simonevertsson.gpu;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.query.QueryGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by simon on 2015-05-12.
 */
public class SpanningTreeGenerator {

    private final QueryGraph queryGraph;
    private final LabelDictionary labelDictionary;
    private HashMap<Long, Boolean> visitedNodes;

    public SpanningTreeGenerator(QueryGraph queryGraph, LabelDictionary labelDictionary, TypeDictionary typeDictionary) {
        this.queryGraph = queryGraph;
        this.labelDictionary = labelDictionary;
    }

    public QueryGraph generateQueryGraph() {
        this.visitedNodes = new HashMap<Long, Boolean>();

        Relationship initialRelationShip = findInitialRelationship(this.queryGraph.relationships, this.labelDictionary);
        this.queryGraph.visitOrder.add(initialRelationShip.getStartNode());
        visitedNodes.put(initialRelationShip.getStartNode().getId(), true);

        while(this.queryGraph.visitOrder.size() < this.queryGraph.nodes.size()) {
            addNextLevelOfNodes();
        }

        return this.queryGraph;
    }

    private void addNextLevelOfNodes() {
        for (Iterator<Relationship> iter = this.queryGraph.relationships.listIterator(); iter.hasNext(); ) {
            Relationship relationship = iter.next();
            long currentStartId = relationship.getStartNode().getId();
            long currentEndId = relationship.getEndNode().getId();
            if(visitedNodes.containsKey(currentStartId) && !visitedNodes.containsKey(currentEndId)) {
                addNodeToSpanningTree(relationship, relationship.getEndNode());
            } else if(!visitedNodes.containsKey(currentStartId) && visitedNodes.containsKey(currentEndId)) {
               addNodeToSpanningTree(relationship, relationship.getStartNode());
            }
        }
    }

    private void addNodeToSpanningTree(Relationship relationship, Node node) {
        this.queryGraph.spanningTree.add(relationship);
        this.queryGraph.visitOrder.add(node);
        this.visitedNodes.put(node.getId(), true);
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

    private static void addNodeLabelsToCount(HashMap<String, Integer> labelCounter, HashSet<Long> visitedNodes, Node node, LabelDictionary labelDictionary) {
        if( !visitedNodes.contains(node.getId()) ) {
            for (Label label : node.getLabels()) {
                labelDictionary.insertLabel(label.name());
                Integer currentLabelCountObject = labelCounter.get(label.name());
                int currentLabelCount = currentLabelCountObject == null ? 0 : currentLabelCountObject;
                labelCounter.put(label.name(), currentLabelCount + 1);
            }
            visitedNodes.add(node.getId());
        }
    }


    private static float calculateNodeRankEstimate(HashMap<String, Integer> labelCounter, Node node) {
        int labelFrequency = 0;
        for(Label label : node.getLabels()) {
            labelFrequency += labelCounter.get(label.name());
        }
        int nodeDegree = node.getDegree(Direction.OUTGOING);
        if(labelFrequency > 0) {
            return nodeDegree / (float) labelFrequency;
        } else {
            return nodeDegree;
        }
    }
}
