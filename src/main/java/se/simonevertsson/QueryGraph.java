package se.simonevertsson;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import javax.management.relation.Relation;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by simon on 2015-05-12.
 */
public class QueryGraph {

    ArrayList<Node> nodes;

    ArrayList<Relationship> relationships;

    ArrayList<Relationship> spanningTree = new ArrayList<Relationship>();

    ArrayList<Node> visitOrder = new ArrayList<Node>();


}
