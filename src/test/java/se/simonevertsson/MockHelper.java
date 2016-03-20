package se.simonevertsson;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import se.simonevertsson.gpu.buffer.BufferContainer;
import se.simonevertsson.gpu.buffer.BufferContainerGenerator;
import se.simonevertsson.gpu.graph.GpuGraph;
import se.simonevertsson.gpu.graph.GpuGraphConverter;
import se.simonevertsson.gpu.kernel.QueryKernels;
import se.simonevertsson.gpu.query.QueryContext;
import se.simonevertsson.gpu.query.dictionary.LabelDictionary;
import se.simonevertsson.gpu.query.dictionary.TypeDictionary;
import se.simonevertsson.runner.*;

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
    //    V  V         V
    //    O ---------> C4
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

    //                 (N1)
    //                  O
    //                /  \
    //              /     \
    //            /        \
    //          /           \
    //        v              v
    // (N3) O <-------------- O (N2)
    public static QueryGraph generateTriangleMockQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode N1 = new QueryNode(0);

        QueryNode N2 = new QueryNode(1);

        QueryNode N3 = new QueryNode(2);

        queryGraph.nodes.add(N1);
        queryGraph.nodes.add(N2);
        queryGraph.nodes.add(N3);

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship N1_N2 = N1.createRelationshipTo(N2, 0, null);
        Relationship N1_N3 = N1.createRelationshipTo(N3, 1, null);
        Relationship N2_N3 = N2.createRelationshipTo(N3, 2, null);

        queryGraph.relationships.add(N1_N2);
        queryGraph.relationships.add(N1_N3);
        queryGraph.relationships.add(N2_N3);

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

    public static MockQuery generateMockQuery() throws IOException {
        QueryGraph queryGraph = generateMockQueryGraph();
        QueryGraph dataGraph = generateMockQueryGraph();
        LabelDictionary labelDictionary = new LabelDictionary();
        TypeDictionary typeDictionary = new TypeDictionary();

        GpuGraphConverter queryGraphConverter = new GpuGraphConverter(queryGraph.nodes, labelDictionary, typeDictionary);
        GpuGraph query = queryGraphConverter.convert();
        GpuGraphConverter dataGraphConverter = new GpuGraphConverter(dataGraph.nodes, labelDictionary, typeDictionary);
        GpuGraph data = dataGraphConverter.convert();



        QueryContext queryContext = new QueryContext(data, query, queryGraph, labelDictionary, typeDictionary);
        QueryKernels queryKernels = new QueryKernels();
        BufferContainer bufferContainer = BufferContainerGenerator.generateBufferContainer(queryContext, queryKernels);

        MockQuery mockQuery = new MockQuery(queryContext, queryKernels, bufferContainer);
        return mockQuery;
    }

    public static MockQuery generateTriangleMockQuery() throws IOException {
        QueryGraph queryGraph = generateTriangleMockQueryGraph();
        QueryGraph dataGraph = generateMockQueryGraph();
        LabelDictionary labelDictionary = new LabelDictionary();
        TypeDictionary typeDictionary = new TypeDictionary();

        GpuGraphConverter queryGraphConverter = new GpuGraphConverter(queryGraph.nodes, labelDictionary, typeDictionary);
        GpuGraph query = queryGraphConverter.convert();
        GpuGraphConverter dataGraphConverter = new GpuGraphConverter(dataGraph.nodes, labelDictionary, typeDictionary);
        GpuGraph data = dataGraphConverter.convert();

        QueryContext queryContext = new QueryContext(data, query, queryGraph, labelDictionary, typeDictionary);
        QueryKernels queryKernels = new QueryKernels();
        BufferContainer bufferContainer = BufferContainerGenerator.generateBufferContainer(queryContext, queryKernels);

        MockQuery mockQuery = new MockQuery(queryContext, queryKernels, bufferContainer);
        return mockQuery;
    }
}
