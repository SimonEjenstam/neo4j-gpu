package se.simonevertsson.experiments;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.query.QueryGraph;
import se.simonevertsson.query.QueryNode;
import se.simonevertsson.query.QueryRelationship;

import java.util.*;

/**
 * Created by simon on 2015-07-13.
 */
public class ExperimentQueryGraphGenerator {

    private final ArrayList<Node> allNodes;
    private final int relationshipCount;
    private final int maxRelationshipsPerLevel;
    private int currentNodeAlias;
    private HashMap<Long, QueryNode> visitedNodes;
    private HashMap<Long, Relationship>  visitedRelationships;
    private Queue<Relationship> relationshipQueue;

    public ExperimentQueryGraphGenerator(ArrayList<Node> allNodes, int relationshipCount, int maxRelationshipsPerLevel) {
        this.allNodes = allNodes;
        this.relationshipCount = relationshipCount;
        this.maxRelationshipsPerLevel = maxRelationshipsPerLevel;
    }

    public QueryGraph generate() {

        QueryGraph queryGraph = null;
        if(relationshipCount > 0 && relationshipCount <= 26) {
            Random random = new Random();
            int startIndex = random.nextInt(this.allNodes.size());
            queryGraph = generateQueryGraphFromGivenStartIndex(startIndex);
        }
        return queryGraph;
    }

    private QueryGraph generateQueryGraphFromGivenStartIndex(int startIndex) {
        int rootNodeAttempts = 0;
        while (rootNodeAttempts < this.allNodes.size()) {
            initializeGenerationAttempt();
            QueryGraph queryGraph = new QueryGraph();

            int currentNodeIndex = (startIndex + rootNodeAttempts) % this.allNodes.size();
            Node rootNode = this.allNodes.get(currentNodeIndex);

            addNodeToQueryGraph(queryGraph, rootNode);
            addNodeRelationshipsToQueue(rootNode);

            if (tryGenerateWithNewRootNode(queryGraph) != null) {
                return queryGraph;
            }

            rootNodeAttempts++;
        }
        return null;
    }

    private QueryGraph tryGenerateWithNewRootNode(QueryGraph queryGraph) {
        while(!this.relationshipQueue.isEmpty()) {
            if(this.visitedRelationships.size() == this.relationshipCount) {
                return queryGraph;
            }

            Relationship currentRelationship = this.relationshipQueue.poll();
            if(this.visitedRelationships.get(currentRelationship.getId()) == null) {
                addRelationshipWithNodesToQueryGraph(queryGraph, currentRelationship);
            }
        }
        return null;
    }

    private void addRelationshipWithNodesToQueryGraph(QueryGraph queryGraph, Relationship currentRelationship) {
        Node unvisitedNode = addUnvisitedNodeToQueryGraph(queryGraph, currentRelationship);
        addNodeRelationshipsToQueue(unvisitedNode);
    }

    private Node addUnvisitedNodeToQueryGraph(QueryGraph queryGraph, Relationship currentRelationship) {
        Node startNode = currentRelationship.getStartNode();
        Node endNode = currentRelationship.getEndNode();
        Node unvisitedNode = null;
        Relationship queryRelationship = null;
        if (visitedNodes.get(startNode.getId()) == null) {
            QueryNode startQueryNode = addNodeToQueryGraph(queryGraph, startNode);
            QueryNode endQueryNode = visitedNodes.get(endNode.getId());

            queryRelationship = startQueryNode.createRelationshipTo(endQueryNode, currentRelationship.getId(), currentRelationship.getType());

            unvisitedNode = startNode;
        } else if (visitedNodes.get(endNode.getId()) == null) {
            QueryNode startQueryNode = visitedNodes.get(startNode.getId());
            QueryNode endQueryNode = addNodeToQueryGraph(queryGraph, endNode);

            queryRelationship = startQueryNode.createRelationshipTo(endQueryNode, currentRelationship.getId(), currentRelationship.getType());

            unvisitedNode = endNode;
        }
        if(unvisitedNode != null) {
            queryGraph.relationships.add(queryRelationship);
            visitedRelationships.put(currentRelationship.getId(), queryRelationship);
        }

        return unvisitedNode;
    }

    private void initializeGenerationAttempt() {
        this.relationshipQueue = new LinkedList<Relationship>();
        this.visitedRelationships = new HashMap<>();
        this.visitedNodes = new HashMap<>();
        this.currentNodeAlias = 65; //start with alias letter 'A'
    }

    private QueryNode addNodeToQueryGraph(QueryGraph queryGraph, Node node) {
        QueryNode queryNode = new QueryNode(node);
        queryGraph.nodes.add(queryNode);
        queryGraph.aliasDictionary.insertAlias(queryNode, String.valueOf((char) this.currentNodeAlias));
        this.currentNodeAlias++;
        visitedNodes.put(queryNode.getId(), queryNode);
        return queryNode;
    }

    private Node getUnvisitedNode(HashSet<Long> visitedNodes, Relationship currentRelationship) {
        Node startNode = currentRelationship.getStartNode();
        Node endNode = currentRelationship.getEndNode();
        Node unvisitedNode = null;
        if (!visitedNodes.contains(startNode.getId())) {
            unvisitedNode = startNode;
        } else if (!visitedNodes.contains(endNode.getId())) {
            unvisitedNode = endNode;
        }
        return unvisitedNode;
    }

    private void addNodeRelationshipsToQueue(Node node) {
        if(node != null) {
            int addedRelationships = 0;
            for (Relationship relationship : node.getRelationships(Direction.BOTH)) {
                if(visitedRelationships.get(relationship.getId()) == null) {
                    relationshipQueue.add(relationship);
                    addedRelationships++;
                    if (addedRelationships >= maxRelationshipsPerLevel) ;
                    {
                        break;
                    }
                }
            }
        }
    }

}
