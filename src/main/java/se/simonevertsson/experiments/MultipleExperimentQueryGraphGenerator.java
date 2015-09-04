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
public class MultipleExperimentQueryGraphGenerator {

    private final ArrayList<Node> allNodes;
    private final int maxDepth;
    private final int maxNodeCount;
    private int currentNodeCount;
    private final int nodeCountDecrease;
    private int currentNodeAlias;
    private HashMap<Long, QueryNode> visitedNodes;
    private HashMap<Long, Relationship>  visitedRelationships;
    private Queue<Relationship> relationshipQueue;

    public MultipleExperimentQueryGraphGenerator(ArrayList<Node> allNodes, int maxNodeCount, int nodeCountDecrease, int maxDepth) {
        this.allNodes = allNodes;
        this.maxNodeCount = maxNodeCount;
        this.nodeCountDecrease = nodeCountDecrease;
        this.maxDepth = maxDepth;
    }

    public List<QueryGraph> generate() {

        List<QueryGraph> queryGraphs = null;
        Random random = new Random();
        int startIndex = random.nextInt(this.allNodes.size());
        System.out.println("Generating query graph from start index " + startIndex);
        queryGraphs = generate(startIndex);

        return queryGraphs;
    }

    private List<QueryGraph> generate(int startIndex) {
        return generateQueryGraphFromGivenStartIndex(startIndex);
    }

    private List<QueryGraph> generateQueryGraphFromGivenStartIndex(int startIndex) {
        int rootNodeAttempts = 0;
        ArrayList<QueryGraph> queryGraphs = new ArrayList<>();

        int rootNodeIndex = 0;

        while (rootNodeAttempts < this.allNodes.size()) {
            initializeGenerationAttempt(this.maxNodeCount);
            QueryGraph queryGraph = new QueryGraph();

            rootNodeIndex = (startIndex + rootNodeAttempts) % this.allNodes.size();
            Node rootNode = this.allNodes.get(rootNodeIndex);

            addNodeToQueryGraph(queryGraph, rootNode);
            List<Relationship> relationshipList = createRelationshipList(rootNode);

            if (generateQueryGraph(queryGraph, rootNode, relationshipList, 0) != null) {
                queryGraphs.add(queryGraph);
                break;
            }

            rootNodeAttempts++;
        }

        System.out.println("Root node found at index: " + rootNodeIndex);

        if(!queryGraphs.isEmpty()) {

            int nodeCount = this.currentNodeCount - this.nodeCountDecrease;
            System.out.println("Generating query graph with " + nodeCount + " nodes.");

            while (nodeCount >= 2) {


                initializeGenerationAttempt(nodeCount);
                QueryGraph queryGraph = new QueryGraph();

                Node rootNode = this.allNodes.get(rootNodeIndex);

                addNodeToQueryGraph(queryGraph, rootNode);
                List<Relationship> relationshipList = createRelationshipList(rootNode);

                if (generateQueryGraph(queryGraph, rootNode, relationshipList, 0) != null) {
                    queryGraphs.add(queryGraph);
                    nodeCount = nodeCount - this.nodeCountDecrease;
                    System.out.println("Generating query graph with " + nodeCount + " nodes.");
                }
            }
        }


        return queryGraphs;
    }

    private QueryGraph generateQueryGraph(QueryGraph queryGraph, Node visitedNode, List<Relationship> relationships, int level) {
        if(level >= this.maxDepth) {
            return queryGraph;
        }
        for(Relationship relationship : relationships) {

            if(this.visitedRelationships.get(relationship.getId()) == null) {
                addRelationshipAndUnvisitedNodeToQueryGraph(queryGraph, relationship);
                if (this.visitedNodes.size() == this.currentNodeCount) {
                    /* The query graph has been generated */
                    return queryGraph;
                } else {
                    Node unvisitedNode = null;
                    if (relationship.getStartNode().getId() == visitedNode.getId()) {
                        unvisitedNode = relationship.getEndNode();
                    } else {
                        unvisitedNode = relationship.getStartNode();
                    }

                    List<Relationship> relationshipList = createRelationshipList(unvisitedNode);
                    generateQueryGraph(queryGraph, unvisitedNode, relationshipList, level + 1);
                }
            }

        }

        if (this.visitedNodes.size() == this.currentNodeCount) {
            /* The query graph has been generated */
            return queryGraph;
        } else {
            return null;
        }
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

    private void initializeGenerationAttempt(int nodeCount) {
        this.relationshipQueue = new LinkedList<Relationship>();
        this.visitedRelationships = new HashMap<>();
        this.visitedNodes = new HashMap<>();
        this.currentNodeAlias = 0; //start with alias letter 'A'
        this.currentNodeCount = nodeCount;
    }

    private QueryNode addNodeToQueryGraph(QueryGraph queryGraph, Node node) {
        QueryNode queryNode = new QueryNode(node);
        queryGraph.nodes.add(queryNode);
        queryGraph.aliasDictionary.insertAlias(queryNode, ExperimentUtils.createAlias(this.currentNodeAlias));
        this.currentNodeAlias++;
        visitedNodes.put(queryNode.getId(), queryNode);
        return queryNode;
    }

    private String createAlias() {
        StringBuilder builder = new StringBuilder();

        int aliasLength = (this.currentNodeAlias / 26) + 1;
        int rest = this.currentNodeAlias;
        for(int i = 0; i < aliasLength; i++) {
            int aliasCharacter = rest % 26;
            builder.append((char)(65 + aliasCharacter));
            rest /= 26;
        }

        return builder.reverse().toString();
    }

//    private void addNodeRelationshipsToQueue(Node node) {
//        if(node != null) {
//            int addedRelationships = 0;
//            for (Relationship relationship : node.getRelationships(Direction.BOTH)) {
//                if(visitedRelationships.get(relationship.getId()) == null) {
//                    relationshipQueue.add(relationship);
//                    addedRelationships++;
//                    if (addedRelationships >= maxRelationshipsPerLevel) ;
//                    {
//                        break;
//                    }
//                }
//            }
//        }
//    }

}
