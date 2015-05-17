package se.simonevertsson;

import com.nativelibs4java.opencl.*;
import org.bridj.Pointer;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import se.simonevertsson.mocking.GraphMock;
import se.simonevertsson.mocking.Properties;
import se.simonevertsson.mocking.Property;

import java.io.File;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by simon on 2015-05-12.
 */
public class Experiments {

    //public static final String DB_PATH = "target/drwho";
    public static final String DB_PATH = "target/foo";
    public static final String DB_CONFIG_PATH = "target";
    GraphDatabaseService graphDb;
    private DatabaseService dbService;
    private CLQueue queue;
    private CLContext context;

    // START SNIPPET: createReltype
    private static enum RelTypes implements RelationshipType
    {
        KNOWS
    }
    // END SNIPPET: createReltype

    public static void main(String[] args) throws IOException {
        Experiments experiments = new Experiments();
        experiments.createDb();


        long tick = System.currentTimeMillis();
        experiments.runCypherQuery();
        long time = System.currentTimeMillis()-tick;
        System.out.println("Runtime: " + time);

        experiments.context = JavaCL.createBestContext();
        experiments.queue = experiments.context.createDefaultQueue();

        tick = System.currentTimeMillis();
        experiments.runExperiment();
        time = System.currentTimeMillis()-tick;
        System.out.println("Runtime: " + time);
        experiments.removeData();
        experiments.shutDown();
        System.out.println("foo");
    }

    private void runCypherQuery() {
        try
        {
            String rows = "";
            Transaction transaction = graphDb.beginTx();
            Result result = graphDb.execute(
                    "MATCH (a3),(a1),(b2),(c4)" +
                    "WHERE (a3)<--(a1)-->(b2) AND (a3)<--(b2)-->(c4)<--(a3)" +
                    "RETURN a1, b2, a3, c4");

            while ( result.hasNext() )
            {
                Map<String,Object> row = result.next();
                for ( Map.Entry<String,Object> column : row.entrySet() )
                {
                    rows += column.getKey() + ": " + column.getValue() + "; ";
                }
                rows += "\n";
            }
            transaction.success();
            System.out.println(rows);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    void runExperiment() throws IOException {


        ResourceIterable<Node> allNodes = dbService.getAllNodes();
        LabelDictionary labelDictionary = new LabelDictionary();

        GpuGraphModel dataGraph = GraphModelConverter.convertNodesToGpuGraphModel(allNodes, labelDictionary);
        QueryGraph query = generateMockQueryGraph();
        SpanningTreeGenerator.generateQueryGraph(query, labelDictionary);
        GpuGraphModel queryGraph = GraphModelConverter.convertNodesToGpuGraphModel(query, labelDictionary);
        //GpuGraphModel queryGraph = GraphModelConverter.convertNodesToGpuGraphModel(query.nodes);


        int[] nodeLabels = dataGraph.getNodeLabels();
        int[] candidateSet = new int[query.visitOrder.size()*nodeLabels.length];
        int n = nodeLabels.length;
        int[] prefixScanArray = new int[n];
        prefixScanArray[0] = 0;


        int orderCounter = 0;

        for(Node queryNode : query.visitOrder) {


            // Create OpenCL input and output buffers
            CLBuffer<Integer>
                    labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataGraph.getNodeLabels()), false),
                    c_set = context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(candidateSet, orderCounter * n, n), false);

            CheckCandidates kernels = new CheckCandidates(context);
            int[] globalSizes = new int[] { n };
            String labelName = queryNode.getLabels().iterator().next().name();
            int queryLabelId = labelDictionary.getIdForLabel(labelName);

            CLEvent checkCandidatesEvent = kernels.check_candidates(queue, labels, queryLabelId, c_set, n, globalSizes, null);

            Pointer<Integer> outPtr = c_set.read(queue, checkCandidatesEvent); // blocks until add_floats finished

            int candidateCount = 0;
            if(outPtr.get(0) == 1) {
                candidateCount++;
            }

            for (int i = 1; i < n; i++) {
                prefixScanArray[i] = prefixScanArray[i-1] + outPtr.get(i-1);
                if(outPtr.get(i) == 1) {
                    candidateCount++;
                }
            }

            int[] candidateArray = new int[candidateCount];

            for (int i = 0; i < n; i++) {
                if(outPtr.get(i) == 1) {
                    candidateArray[prefixScanArray[i]] = i;
                    System.out.println("Candidate for query node " + queryNode.getId() + ": " + i);
                }
            }


            orderCounter++;
        }
    }

    void createDb() throws IOException
    {
        //        DatabaseService dbService = new DatabaseService(DB_PATH, DB_CONFIG_PATH);
        //        Iterable<Relationship> allRelationships = dbService.getAllRelationships();
        //        for(Relationship relationship : allRelationships)  {
        //            System.out.println(relationship);
        //        }
        FileUtils.deleteRecursively(new File(DB_PATH));

        // START SNIPPET: startDb
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        registerShutdownHook( graphDb );
        dbService = new DatabaseService(graphDb);
        // END SNIPPET: startDb

        // START SNIPPET: transaction
        try
        {
            Transaction tx = graphDb.beginTx();


            Label labelA = DynamicLabel.label("A");
            Label labelB = DynamicLabel.label("B");
            Label labelC = DynamicLabel.label("C");
            Label labelD = DynamicLabel.label("D");

            Node A1 = graphDb.createNode();
            Node B2 = graphDb.createNode();
            Node A3 = graphDb.createNode();
            Node C4 = graphDb.createNode();

            A1.addLabel(labelA);
            B2.addLabel(labelB);
            A3.addLabel(labelA);
            C4.addLabel(labelC);

            Relationship A1_B2 = A1.createRelationshipTo(B2, RelTypes.KNOWS);
            Relationship A1_A3 = A1.createRelationshipTo(A3, RelTypes.KNOWS);
            Relationship B2_A3 = B2.createRelationshipTo(A3, RelTypes.KNOWS);
            Relationship B2_C4 = B2.createRelationshipTo(C4, RelTypes.KNOWS);
            Relationship A3_C4 = A3.createRelationshipTo(C4, RelTypes.KNOWS);

            // START SNIPPET: transaction
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // END SNIPPET: transaction
    }

    void removeData()
    {
        this.dbService.deleteData();
    }

    void shutDown()
    {
        System.out.println();
        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }

    // START SNIPPET: shutdownHook
    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
    // END SNIPPET: shutdownHook

//    A1 ----> B2
//    |      / |
//    |    /   |
//    V  y     V
//    A3 ----> C4
    private static QueryGraph generateMockQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        Properties props = Properties.properties(Property.property("foo", "bar"));
        Node A1 = GraphMock.node(1, props, 2, "A");
        Node B2 = GraphMock.node(2, props, 2, "B");
        Node A3 = GraphMock.node(3, props, 1, "A");
        Node C4 = GraphMock.node(4, props, 0, "C");

        queryGraph.nodes.add(A1);
        queryGraph.nodes.add(B2);
        queryGraph.nodes.add(A3);
        queryGraph.nodes.add(C4);

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A1_B2 = GraphMock.relationship(1, props, A1, "test", B2);
        Relationship A1_A3 = GraphMock.relationship(2, props, A1, "test", A3);
        Relationship B2_A3 = GraphMock.relationship(3, props, B2, "test", A3);
        Relationship B2_C4 = GraphMock.relationship(4, props, B2, "test", C4);
        Relationship A3_C4 = GraphMock.relationship(5, props, A3, "test", C4);

        queryGraph.relationships.add(A1_B2);
        queryGraph.relationships.add(A1_A3);
        queryGraph.relationships.add(B2_A3);
        queryGraph.relationships.add(B2_C4);
        queryGraph.relationships.add(A3_C4);

        return queryGraph;
    }

//    Doctor ----> Master

//    private static QueryGraph generateMockQueryGraph() {
//
//        QueryGraph queryGraph = new QueryGraph();
//
//        queryGraph.nodes = new ArrayList<Node>();
//
//        Properties props = Properties.properties(Property.property("foo", "bar"));
//        Node doctor = GraphMock.node(1, props, 1, "Doctor");
//        Node master = GraphMock.node(2, props, 0, "Master");
//
//        queryGraph.nodes.add(doctor);
//        queryGraph.nodes.add(master);
//
//        queryGraph.relationships = new ArrayList<Relationship>();
//
//        Relationship A1_B2 = GraphMock.relationship(1, props, doctor, "test", B2);
//
//        queryGraph.relationships.add(A1_B2);
//        queryGraph.relationships.add(A1_A3);
//        queryGraph.relationships.add(B2_A3);
//        queryGraph.relationships.add(B2_C4);
//        queryGraph.relationships.add(A3_C4);
//
//        return queryGraph;
//    }

}
