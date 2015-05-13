package se.simonevertsson;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by simon on 2015-05-12.
 */
public class QueryGraph {

    ArrayList<Node> nodes;

    ArrayList<Relationship> relationships;

    ArrayList<Long> spanningTree = new ArrayList<Long>();

    ArrayList<Long> visitOrder = new ArrayList<Long>();


}
