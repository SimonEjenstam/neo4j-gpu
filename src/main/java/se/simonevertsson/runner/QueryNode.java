package se.simonevertsson.runner;

import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by simon.evertsson on 2015-05-18.
 */
public class QueryNode implements Node {

    private final long id;

    private ArrayList<Relationship> outgoingRelationsships;
    private ArrayList<Relationship> incomingRelationsships;
    private HashSet<Label> labels;

    public QueryNode(long id) {
        this.id = id;
        this.outgoingRelationsships = new ArrayList<Relationship>();
        this.incomingRelationsships = new ArrayList<>();
        this.labels = new HashSet<Label>();
    }

    public QueryNode(Node node) {
        this.id = node.getId();
        this.outgoingRelationsships = new ArrayList<Relationship>();
        this.incomingRelationsships = new ArrayList<>();
        this.labels = new HashSet<>();
        for(Label label : node.getLabels()) {
            this.addLabel(label);
        }
    }

    public long getId() {
        return this.id;
    }

    public void delete() {

    }

    public Iterable<Relationship> getRelationships() {
        return this.outgoingRelationsships;
    }

    public boolean hasRelationship() {
        return false;
    }

    public Iterable<Relationship> getRelationships(RelationshipType... relationshipTypes) {
        return null;
    }

    public Iterable<Relationship> getRelationships(Direction direction, RelationshipType... relationshipTypes) {
        return null;
    }

    public boolean hasRelationship(RelationshipType... relationshipTypes) {
        return false;
    }

    public boolean hasRelationship(Direction direction, RelationshipType... relationshipTypes) {
        return false;
    }

    public Iterable<Relationship> getRelationships(Direction direction) {
        return outgoingRelationsships;
    }

    public boolean hasRelationship(Direction direction) {
        return false;
    }

    public Iterable<Relationship> getRelationships(RelationshipType relationshipType, Direction direction) {
        return null;
    }

    public boolean hasRelationship(RelationshipType relationshipType, Direction direction) {
        return false;
    }

    public Relationship getSingleRelationship(RelationshipType relationshipType, Direction direction) {
        return null;
    }

    public Relationship createRelationshipTo(Node node, RelationshipType relationshipType) {
        return null;
    }

    public Relationship createRelationshipTo(Node node, long relationshipId, RelationshipType relationshipType) {
        Relationship relationship = new QueryRelationship(relationshipId, this, node, relationshipType);
        this.outgoingRelationsships.add(relationship);
        ((QueryNode)node).getIncomingRelationsships().add(relationship);
        return relationship;
    }

    public Iterable<RelationshipType> getRelationshipTypes() {
        return null;
    }

    public int getDegree() {
        return this.outgoingRelationsships.size() + this.incomingRelationsships.size();
    }

    public int getDegree(RelationshipType relationshipType) {
        return 0;
    }

    public int getDegree(Direction direction) {
        /* We assume that the nodes in runner graph only have outgoing relationships */
        return this.outgoingRelationsships.size();
    }

    public int getDegree(RelationshipType relationshipType, Direction direction) {
        return 0;
    }

    public Traverser traverse(Traverser.Order order, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType, Direction direction) {
        return null;
    }

    public Traverser traverse(Traverser.Order order, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType, Direction direction, RelationshipType relationshipType1, Direction direction1) {
        return null;
    }

    public Traverser traverse(Traverser.Order order, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, Object... objects) {
        return null;
    }

    public void addLabel(Label label) {
        this.labels.add(label);
    }

    public void removeLabel(Label label) {

    }

    public boolean hasLabel(Label label) {
        return false;
    }

    public Iterable<Label> getLabels() {
        return this.labels;
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

    public ArrayList<Relationship> getIncomingRelationsships() {
        return incomingRelationsships;
    }
}
