package se.simonevertsson;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import se.simonevertsson.gpu.*;
import se.simonevertsson.query.QueryGraph;
import se.simonevertsson.query.QueryGraphGenerator;
import se.simonevertsson.query.QueryLabel;
import se.simonevertsson.query.QueryNode;

import java.awt.image.Kernel;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by simon.evertsson on 2015-05-28.
 */
public class MockHelper {

    private static enum RelTypes implements RelationshipType
    {
        KNOWS,
        LOVES
    }


    //    A1 --------> B2
    //    |          / |
    //  KNOWS      /   |
    //    |      /   LOVES
    //    |    /       |
    //    V  y         V
    //    O -------> C4
    public static QueryGraph generateMockQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode A1 = new QueryNode(0);
        A1.addLabel(new QueryLabel("A"));

        QueryNode B2 = new QueryNode(1);
        B2.addLabel(new QueryLabel("B"));

        QueryNode unlabeled = new QueryNode(2);

        QueryNode C4 = new QueryNode(3);
        C4.addLabel(new QueryLabel("C"));

        queryGraph.nodes.add(A1);
        queryGraph.nodes.add(B2);
        queryGraph.nodes.add(unlabeled);
        queryGraph.nodes.add(C4);

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A1_B2 = A1.createRelationshipTo(B2, 0, null);
        Relationship A1_unlabeled = A1.createRelationshipTo(unlabeled, 1, RelTypes.KNOWS);
        Relationship B2_unlabeled = B2.createRelationshipTo(unlabeled, 2, null);
        Relationship B2_C4 = B2.createRelationshipTo(C4, 3, RelTypes.LOVES);
        Relationship unlabeled_C4 = unlabeled.createRelationshipTo(C4, 4, null);

        queryGraph.relationships.add(A1_B2);
        queryGraph.relationships.add(A1_unlabeled);
        queryGraph.relationships.add(B2_unlabeled);
        queryGraph.relationships.add(B2_C4);
        queryGraph.relationships.add(unlabeled_C4);

        return queryGraph;
    }

    //    A1 ----> B2
    //    |      / |
    //    |    /   |
    //    V  y     V
    //    A3 ----> C4
    public static QueryGraph generateBasicMockQueryGraph() {

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

        Relationship A1_B2 = A1.createRelationshipTo(B2, 0, RelTypes.KNOWS);
        Relationship A1_A3 = A1.createRelationshipTo(A3, 1, RelTypes.KNOWS);
        Relationship B2_A3 = B2.createRelationshipTo(A3, 2, RelTypes.KNOWS);
        Relationship B2_C4 = B2.createRelationshipTo(C4, 3, RelTypes.KNOWS);
        Relationship A3_C4 = A3.createRelationshipTo(C4, 4, RelTypes.KNOWS);

        queryGraph.relationships.add(A1_B2);
        queryGraph.relationships.add(A1_A3);
        queryGraph.relationships.add(B2_A3);
        queryGraph.relationships.add(B2_C4);
        queryGraph.relationships.add(A3_C4);

        return queryGraph;
    }

    int[] queryRelationships = {
            1, 2, 2, 3, 3, -1
    };

    int[] queryRelationshipTypes = {
            -1, 1, -1, 2, -1, -1
    };

    int[] queryRelationshipIndices = {
            0, 2, 4, 5, 6,
    };

    int[] queryLabels = {
            1,
            2,
            1,
            3
    };

    int[] queryLabelIndicies = {
            0,
            1,
            2,
            3,
            4
    };

    int[] dataRelationships = {
            1, 2, 2, 3, 3, -1
    };

    int[] dataRelationshipTypes = {
            -1, 1, -1, 2, -1, -1
    };

    int[] dataRelationshipIndices = {
            0, 2, 4, 5, 6,
    };

    int[] dataLabels = {
            1,
            2,
            1,
            3
    };

    int[] dataLabelIndicies = {
            0,
            1,
            2,
            3,
            4
    };


    public static MockQuery generateMockQuery() throws IOException {
        QueryGraph queryGraph = generateMockQueryGraph();
        QueryGraph dataGraph = generateMockQueryGraph();
        LabelDictionary labelDictionary = new LabelDictionary();
        TypeDictionary typeDictionary = new TypeDictionary();

        GraphModelConverter queryGraphConverter = new GraphModelConverter(queryGraph.nodes, labelDictionary, typeDictionary);
        GpuGraphModel query = queryGraphConverter.convert();
        GraphModelConverter dataGraphConverter = new GraphModelConverter(dataGraph.nodes, labelDictionary, typeDictionary);
        GpuGraphModel data = dataGraphConverter.convert();

        QueryContext queryContext = new QueryContext(data, query, queryGraph, labelDictionary, typeDictionary);
        QueryKernels queryKernels = new QueryKernels();
        BufferContainer bufferContainer = BufferContainerGenerator.generateBufferContainer(queryContext, queryKernels);

        MockQuery mockQuery = new MockQuery(queryContext, queryKernels, bufferContainer);
        return mockQuery;
    }
}
