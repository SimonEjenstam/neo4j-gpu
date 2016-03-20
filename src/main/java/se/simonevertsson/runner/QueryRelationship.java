package se.simonevertsson.runner;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class QueryRelationship implements Relationship {

  private final long id;
  private final Node startNode;
  private final Node endNode;
  private final RelationshipType type;

  public QueryRelationship(long id, Node startNode, Node endNode, RelationshipType type) {
    this.id = id;
    this.startNode = startNode;
    this.endNode = endNode;
    this.type = type;
  }

  public long getId() {
    return this.id;
  }

  public void delete() {

  }

  public Node getStartNode() {
    return this.startNode;
  }

  public Node getEndNode() {
    return this.endNode;
  }

  public Node getOtherNode(Node node) {
    return null;
  }

  public Node[] getNodes() {
    return new Node[0];
  }

  public RelationshipType getType() {
    return this.type;
  }

  public boolean isType(RelationshipType relationshipType) {
    return false;
  }

  public GraphDatabaseService getGraphDatabase() {
    return null;
  }

  public boolean hasProperty(String s) {
    return false;
  }

  public Object getProperty(String s) {
    return null;
  }

  public Object getProperty(String s, Object o) {
    return null;
  }

  public void setProperty(String s, Object o) {

  }

  public Object removeProperty(String s) {
    return null;
  }

  public Iterable<String> getPropertyKeys() {
    return null;
  }
}
