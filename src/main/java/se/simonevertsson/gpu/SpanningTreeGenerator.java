package se.simonevertsson.gpu;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.query.QueryGraph;

import java.util.*;

/**
 * Created by simon on 2015-05-12.
 */
public class SpanningTreeGenerator {

    private HashMap<Long, Boolean> visitedNodes;
    private List<Relationship> spanningTreeRelationships;
    private List<Node> spanningTreeVisitOrder;


    public SpanningTree generate(QueryGraph queryGraph) {
        this.spanningTreeRelationships = new ArrayList<>();
        this.spanningTreeVisitOrder = new ArrayList<>();
        this.visitedNodes = new HashMap<Long, Boolean>();

        Relationship initialRelationShip = findInitialRelationship(queryGraph.relationships);
        this.spanningTreeVisitOrder.add(initialRelationShip.getStartNode());
        visitedNodes.put(initialRelationShip.getStartNode().getId(), true);

        while(this.spanningTreeVisitOrder.size() < queryGraph.nodes.size()) {
            addNextLevelOfNodes(queryGraph);
        }

        return new SpanningTree(this.spanningTreeRelationships, this.spanningTreeVisitOrder);
    }

    private void addNextLevelOfNodes(QueryGraph queryGraph) {
        for (Iterator<Relationship> iter = queryGraph.relationships.listIterator(); iter.hasNext(); ) {
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
        this.spanningTreeRelationships.add(relationship);
        this.spanningTreeVisitOrder.add(node);
        this.visitedNodes.put(node.getId(), true);
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

    private static void addNodeLabelsToCount(HashMap<String, Integer> labelCounter, HashSet<Long> visitedNodes, Node node) {
        if( !visitedNodes.contains(node.getId()) ) {
            for (Label label : node.getLabels()) {
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
