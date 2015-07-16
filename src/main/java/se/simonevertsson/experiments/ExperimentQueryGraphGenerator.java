package se.simonevertsson.experiments;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.query.QueryGraph;
import se.simonevertsson.query.QueryNode;

import java.util.*;

/**
 * Created by simon on 2015-07-13.
 */
public class ExperimentQueryGraphGenerator {

    private final ArrayList<Node> allNodes;
    private final int relationshipCount;
    private final int maxRelationshipsPerLevel;
    private final int nodeCount;
    private final int maxDepth;
    private int currentNodeAlias;
    private HashMap<Long, QueryNode> visitedNodes;
    private HashMap<Long, Relationship>  visitedRelationships;
    private Queue<Relationship> relationshipQueue;

    public ExperimentQueryGraphGenerator(ArrayList<Node> allNodes, int relationshipCount, int nodeCount, int maxRelationshipsPerLevel, int maxDepth) {
        this.allNodes = allNodes;
        this.relationshipCount = relationshipCount;
        this.nodeCount = nodeCount;
        this.maxRelationshipsPerLevel = maxRelationshipsPerLevel;
        this.maxDepth = maxDepth;
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
            List<Relationship> relationshipList = createRelationshipList(rootNode);

            if (generateQueryGraph(queryGraph, rootNode, relationshipList, 0) != null) {
                return queryGraph;
            }

//            if (tryGenerateWithNewRootNode(queryGraph) != null) {
//                return queryGraph;
//            }

            rootNodeAttempts++;
        }
        return null;
    }

    private QueryGraph generateQueryGraph(QueryGraph queryGraph, Node visitedNode, List<Relationship> relationships, int level) {
        if(relationships == null || relationships.isEmpty()) {
            return null;
        }
        if(level < this.maxDepth) {
            for(Relationship relationship : relationships) {

                if(this.visitedRelationships.get(relationship.getId()) == null) {
                    addRelationshipAndUnvisitedNodeToQueryGraph(queryGraph, relationship);
                    if (this.visitedRelationships.size() == this.relationshipCount && this.visitedNodes.size() == this.nodeCount) {
                        return queryGraph;
                    }
                    Node unvisitedNode = null;
                    if(relationship.getStartNode().getId() == visitedNode.getId()) {
                        unvisitedNode = relationship.getEndNode();
                    } else {
                        unvisitedNode = relationship.getStartNode();
                    }

                    List<Relationship> relationshipList = createRelationshipList(unvisitedNode);
                    if(generateQueryGraph(queryGraph, unvisitedNode, relationshipList, level+1) != null) {
                        return queryGraph;
                    }
                }

            }
        }
        return null;
    }

    private List<Relationship> createRelationshipList(Node node) {
        List<Relationship> relationshipList = new ArrayList<>();
        if(node != null) {
            int addedRelationships = 0;
            for (Relationship relationship : node.getRelationships(Direction.BOTH)) {
                if(visitedRelationships.get(relationship.getId()) == null) {
                    relationshipList.add(relationship);
                    addedRelationships++;
                }
            }
        }
        return relationshipList;
    }

//    private QueryGraph tryGenerateWithNewRootNode(QueryGraph queryGraph) {
//        while(!this.relationshipQueue.isEmpty()) {
//            if(this.visitedRelationships.size() == this.relationshipCount) {
//                return queryGraph;
//            }
//
//            Relationship currentRelationship = this.relationshipQueue.poll();
//            if(this.visitedRelationships.get(currentRelationship.getId()) == null) {
//                addRelationshipWithNodesToQueryGraph(queryGraph, currentRelationship);
//            }
//        }
//        return null;
//    }

//    private void addRelationshipWithNodesToQueryGraph(QueryGraph queryGraph, Relationship currentRelationship) {
//        Node unvisitedNode = addRelationshipWithUnvisitedNodeToQueryGraph(queryGraph, currentRelationship);
//        addNodeRelationshipsToQueue(unvisitedNode);
//    }

    private void addRelationshipAndUnvisitedNodeToQueryGraph(QueryGraph queryGraph, Relationship currentRelationship) {
        Node startNode = currentRelationship.getStartNode();
        Node endNode = currentRelationship.getEndNode();
        QueryNode startQueryNode;
        QueryNode endQueryNode;
        if (visitedNodes.get(startNode.getId()) == null) {
            startQueryNode = addNodeToQueryGraph(queryGraph, startNode);
            endQueryNode = visitedNodes.get(endNode.getId());

        } else if (visitedNodes.get(endNode.getId()) == null) {
            startQueryNode = visitedNodes.get(startNode.getId());
            endQueryNode = addNodeToQueryGraph(queryGraph, endNode);
        } else {
            startQueryNode = visitedNodes.get(startNode.getId());
            endQueryNode = visitedNodes.get(endNode.getId());
        }


        Relationship queryRelationship = startQueryNode.createRelationshipTo(endQueryNode, currentRelationship.getId(), currentRelationship.getType());

        queryGraph.relationships.add(queryRelationship);
        visitedRelationships.put(currentRelationship.getId(), queryRelationship);
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
