package se.simonevertsson.query;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.experiments.RelationshipTypes;

import java.util.ArrayList;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class QueryGraphGenerator {

//    A1 ----> B2
//    |      / |
//    |    /   |
//    V  y     V
//    A3 ----> C4
    public static QueryGraph generateMockQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode A1 = new QueryNode(0);
        A1.addLabel(new QueryLabel("A"));

        QueryNode B2 = new QueryNode(1);
        B2.addLabel(new QueryLabel("B"));

        QueryNode A3 = new QueryNode(2);
        A3.addLabel(new QueryLabel("A"));

        QueryNode C4 = new QueryNode(3);
        C4.addLabel(new QueryLabel("C"));

        queryGraph.nodes.add(A1);
        queryGraph.nodes.add(B2);
        queryGraph.nodes.add(A3);
        queryGraph.nodes.add(C4);

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A1_B2 = A1.createRelationshipTo(B2, 0, RelationshipTypes.KNOWS);
        Relationship A1_A3 = A1.createRelationshipTo(A3, 1, RelationshipTypes.KNOWS);
        Relationship B2_A3 = B2.createRelationshipTo(A3, 2, RelationshipTypes.KNOWS);
        Relationship B2_C4 = B2.createRelationshipTo(C4, 3, RelationshipTypes.KNOWS);
        Relationship A3_C4 = A3.createRelationshipTo(C4, 4, RelationshipTypes.KNOWS);

        queryGraph.relationships.add(A1_B2);
        queryGraph.relationships.add(A1_A3);
        queryGraph.relationships.add(B2_A3);
        queryGraph.relationships.add(B2_C4);
        queryGraph.relationships.add(A3_C4);

        return queryGraph;
    }
}
