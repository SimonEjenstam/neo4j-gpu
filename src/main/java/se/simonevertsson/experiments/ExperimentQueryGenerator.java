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
public class ExperimentQueryGenerator {

    public static QueryGraph generateExperimentQuery(ArrayList<Node> allNodes, int relationshipCount, int maxRelationshipsPerLevel) {

        if(relationshipCount > 0 && relationshipCount <= 26) {
            Random random = new Random();
            int startIndex = random.nextInt(allNodes.size());
            int rootNodeAttempts = 0;
            while (rootNodeAttempts < allNodes.size()) {
                Queue<Relationship> relationshipQueue = new LinkedList<Relationship>();
                HashMap<Long, Relationship> visitedRelationships = new HashMap<>();
                HashMap<Long, QueryNode> visitedNodes = new HashMap<>();
                QueryGraph queryGraph = new QueryGraph();
                int currentNodeAlias = 65; //start with alias letter 'A'


                int currentNodeIndex = (startIndex + rootNodeAttempts) % allNodes.size();
                Node rootNode = allNodes.get(currentNodeIndex);

                QueryNode rootQueryNode = new QueryNode(rootNode);
                queryGraph.nodes.add(rootQueryNode);
                queryGraph.aliasDictionary.insertAlias(rootQueryNode, String.valueOf((char) currentNodeAlias));
                currentNodeAlias++;
                visitedNodes.put(rootQueryNode.getId(), rootQueryNode);


                addNodeRelationshipsToQueue(relationshipQueue, visitedRelationships, rootNode, maxRelationshipsPerLevel);

                while(!relationshipQueue.isEmpty()) {
                    if(visitedRelationships.size() == relationshipCount) {
                        return queryGraph;
                    }

                    Relationship currentRelationship = relationshipQueue.poll();
                    if(visitedRelationships.get(currentRelationship.getId()) != null) {
                        Node startNode = currentRelationship.getStartNode();
                        Node endNode = currentRelationship.getEndNode();
                        QueryNode unvisitedNode = null;
                        if (visitedNodes.get(startNode.getId()) == null) {
                            QueryNode startQueryNode = new QueryNode(startNode);
                            QueryNode endQueryNode = new QueryNode(visitedNodes);
                            queryGraph.nodes.add(startQueryNode);
                            queryGraph.aliasDictionary.insertAlias(startQueryNode, String.valueOf((char) currentNodeAlias));
                            currentNodeAlias++;
                            visitedNodes.put(startQueryNode.getId(), startQueryNode);

                            startQueryNode.

                            unvisitedNode = startQueryNode;
                        } else if (visitedNodes.get(endNode.getId()) == null) {
                            rootQueryNode = new QueryNode(endNode);
                            queryGraph.nodes.add(rootQueryNode);
                            queryGraph.aliasDictionary.insertAlias(rootQueryNode, String.valueOf((char) currentNodeAlias));
                            currentNodeAlias++;
                            visitedNodes.put(rootQueryNode.getId(), rootQueryNode);
                            unvisitedNode = rootQueryNode;
                        }

                        addNodeRelationshipsToQueue(relationshipQueue, visitedRelationships, unvisitedNode, maxRelationshipsPerLevel);

                        if(unvisitedNode != null) {
                            queryGraph.nodes.add(unvisitedNode);
                            queryGraph.aliasDictionary.insertAlias(unvisitedNode, String.valueOf((char) currentNodeAlias));
                            currentNodeAlias++;
                            visitedNodes.add(unvisitedNode.getId());
                        }

                        queryGraph.relationships.add(currentRelationship);
                        visitedRelationships.add(currentRelationship.getId());
                    }
                }

                rootNodeAttempts++;
            }
        }
        return null;
    }

    private static Node getUnvisitedNode(HashSet<Long> visitedNodes, Relationship currentRelationship) {
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

    private static void addNodeRelationshipsToQueue(Queue<Relationship> relationshipQueue, HashMap<Long, Relationship> visitedRelationships, Node node, int maxRelationshipsPerLevel) {
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
