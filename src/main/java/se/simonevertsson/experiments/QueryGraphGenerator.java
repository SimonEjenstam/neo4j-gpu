package se.simonevertsson.experiments;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.query.QueryGraph;
import se.simonevertsson.query.QueryNode;

import java.util.*;

/**
 * This class tries to create a {@link List<QueryGraph>} generated from the supplied database ({@link List<Node>}), which fits
 * the supplied graph characteristics.
 */
public class QueryGraphGenerator {

  private final List<Node> allNodes;
  private final int preferredRelationshipCount;
  private final int preferredNodeCount;
  private int currentNodeCount;
  private int currentNodeAlias;
  private HashMap<Long, QueryNode> visitedNodes;
  private HashMap<Long, Relationship> visitedRelationships;
  private Queue<Relationship> relationshipQueue;
  private int currentMinRelationships;
  private int preferredIterationCount;
  private int minimumQueryGraphAmount;

  /**
   * Creates a new generator which will try to generate query graphs from existing sub graphs in the supplied database.
   * The characteristics of the query graphs which will be attempted to be generated can be controlled with the supplied
   * parameters.
   *
   * @param allNodes The database represented as a list of node
   * @param preferredNodeCount The preferred amount of nodes in the query graphs.
   * @param preferredRelationshipCount The preferred amount of relationships in the query graph. If the amount of
   * generated query graphs is less than what's set with {@link #setMinimumQueryGraphAmount(int)} (default 10), the
   * prefered amount of relationships is decreased to widen the search area.
   */
  public QueryGraphGenerator(List<Node> allNodes, int preferredNodeCount, int preferredRelationshipCount) {
    this.allNodes = allNodes;
    this.preferredNodeCount = preferredNodeCount;
    this.preferredRelationshipCount = preferredRelationshipCount;
    this.minimumQueryGraphAmount = 10; // Default minimum query graph amount (Used to get better statistical results)
  }

  public void setMinimumQueryGraphAmount(int minimumQueryGraphAmount) {
    this.minimumQueryGraphAmount = minimumQueryGraphAmount;
  }

  public ArrayList<QueryGraph> generate(int preferredIterationCount) {
    this.preferredIterationCount = Integer.max(preferredIterationCount, 10);

    ArrayList<QueryGraph> queryGraphs = null;
    Random random = new Random();
    int startIndex = random.nextInt(this.allNodes.size());
    System.out.println("Generating query graph from start index " + startIndex);
    queryGraphs = generateQueryGraphFromGivenStartIndex(startIndex);

    return queryGraphs;
  }


  private ArrayList<QueryGraph> generateQueryGraphFromGivenStartIndex(int startIndex) {
    int rootNodeAttempts = 0;
    ArrayList<QueryGraph> queryGraphs = new ArrayList<>();

    int rootNodeIndex;
    QueryGraph queryGraph;
    this.currentMinRelationships = this.preferredRelationshipCount;
    while (this.currentMinRelationships >= this.preferredNodeCount - 1) {
      while (rootNodeAttempts < this.allNodes.size() &&
          queryGraphs.size() < (this.preferredIterationCount)) {
        initializeGenerationAttempt(this.preferredNodeCount);
        queryGraph = new QueryGraph();

        rootNodeIndex = (startIndex + rootNodeAttempts) % this.allNodes.size();
        Node rootNode = this.allNodes.get(rootNodeIndex);

        addNodeToQueryGraph(queryGraph, rootNode);

        if (generateQueryGraph(queryGraph, rootNode) != null && queryGraph.relationships.size() >= this.currentMinRelationships && queryGraph.nodes.size() == this.preferredNodeCount) {
          if (!queryGraphs.contains(queryGraph)) {
            System.out.println("Generated query graph with " + this.currentNodeCount + " nodes and minimum " + this.currentMinRelationships + " relationships.");
            queryGraphs.add(queryGraph);
          }
        }

        rootNodeAttempts++;
      }

      if (queryGraphs.size() < minimumQueryGraphAmount) {

        this.currentMinRelationships--;
        System.out.println("Too few query graphs (" + queryGraphs.size() + "), generating with " + this.currentMinRelationships + " relationships");
        queryGraphs = new ArrayList<>();
        rootNodeAttempts = 0;
      } else {
        break;
      }
    }

    return queryGraphs;
  }

  private QueryGraph generateQueryGraph(QueryGraph queryGraph, Node visitedNode) {
    for (Relationship relationship : visitedNode.getRelationships()) {
      if (this.visitedNodes.size() == this.currentNodeCount && this.visitedRelationships.size() >= this.preferredRelationshipCount) {
        // The query graph has been generated
        return queryGraph;
      }

      if (this.visitedRelationships.get(relationship.getId()) == null) {
        if (this.visitedNodes.size() == this.currentNodeCount) {

          // Add relationships until stopping condition is fulfilled
          addRelationshipWithVisitedNodesToQueryGraph(queryGraph, relationship);
        } else if (this.visitedRelationships.size() < this.preferredRelationshipCount) {

          // Add nodes and relationships until stopping condition is fulfilled
          addRelationshipAndUnvisitedNodeToQueryGraph(queryGraph, relationship);
          Node unvisitedNode;
          if (relationship.getStartNode().getId() == visitedNode.getId()) {
            unvisitedNode = relationship.getEndNode();
          } else {
            unvisitedNode = relationship.getStartNode();
          }
          generateQueryGraph(queryGraph, unvisitedNode);
        }

      }

    }

    if (this.visitedNodes.size() == this.currentNodeCount) {
            /* The query graph has been generated */
      return queryGraph;
    } else {
      if (this.visitedRelationships.size() >= this.preferredRelationshipCount) {
        return queryGraph;
      } else {
        return null;
      }
    }
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

  private void addRelationshipWithVisitedNodesToQueryGraph(QueryGraph queryGraph, Relationship relationship) {
    QueryNode startQueryNode = visitedNodes.get(relationship.getStartNode().getId());
    QueryNode endQueryNode = visitedNodes.get(relationship.getEndNode().getId());
    if (startQueryNode != null && endQueryNode != null) {
      Relationship queryRelationship = startQueryNode.createRelationshipTo(endQueryNode, relationship.getId(), relationship.getType());
      queryGraph.relationships.add(queryRelationship);
      visitedRelationships.put(relationship.getId(), queryRelationship);
    }
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
    for (int i = 0; i < aliasLength; i++) {
      int aliasCharacter = rest % 26;
      builder.append((char) (65 + aliasCharacter));
      rest /= 26;
    }

    return builder.reverse().toString();
  }
}
