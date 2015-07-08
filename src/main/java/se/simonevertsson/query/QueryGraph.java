package se.simonevertsson.query;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import java.util.ArrayList;

/**
 * Created by simon on 2015-05-12.
 */
public class QueryGraph {

    public ArrayList<Node> nodes;

    public ArrayList<Relationship> relationships;

    public ArrayList<Relationship> spanningTree = new ArrayList<Relationship>();

    public ArrayList<Node> visitOrder = new ArrayList<Node>();

    public AliasDictionary aliasDictionary = new AliasDictionary();
}
