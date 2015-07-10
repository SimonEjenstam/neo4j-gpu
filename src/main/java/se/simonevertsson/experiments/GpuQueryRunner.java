package se.simonevertsson.experiments;

import com.nativelibs4java.opencl.CLBuffer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.gpu.*;
import se.simonevertsson.query.QueryGraph;
import se.simonevertsson.query.QueryGraphGenerator;

import javax.management.Query;
import java.io.IOException;
import java.util.List;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class GpuQueryRunner {

    public List<QuerySolution> runGpuQuery(DatabaseService databaseService) throws IOException {
        long tick, tock;

        /* Convert database data and query data to fit the GPU */
        tick = System.currentTimeMillis();
        LabelDictionary labelDictionary = new LabelDictionary();
        TypeDictionary typeDictionary = new TypeDictionary();
//        QueryGraph queryGraph = QueryGraphGenerator.generateUnlabeledMockQueryGraph();
        QueryGraph queryGraph = QueryGraphGenerator.generateTriangleMockQueryGraph();

        GpuGraphModel gpuData = convertData(databaseService, labelDictionary, typeDictionary);
        GpuGraphModel gpuQuery = convertQuery(labelDictionary, typeDictionary, queryGraph);
        tock = System.currentTimeMillis();

        System.out.println("GPU Data conversion runtime: " + (tock-tick) + "ms");
//        System.out.println("------Query-----");
//        System.out.println(gpuQuery.toString());
//        System.out.println("------Data-----");
//        System.out.println(gpuData.toString());

        /* Execute the query */
        QueryContext queryContext = new QueryContext(gpuData, gpuQuery, queryGraph, labelDictionary, typeDictionary);
        GpuQuery gpuGraphQuery = new GpuQuery(queryContext);
        tick = System.currentTimeMillis();
        List<QuerySolution> results = gpuGraphQuery.executeQuery(queryGraph.visitOrder);
        tock = System.currentTimeMillis();

//        for(String result : results) {
//            System.out.println(result);
//        }
//        System.out.println("Number of solutions: " + results.size());

        System.out.println("GPU Query runtime: " + (tock - tick) + "ms");

        return results;
    }

    private GpuGraphModel convertQuery(LabelDictionary labelDictionary, TypeDictionary typeDictionary, QueryGraph queryGraph) {
        SpanningTreeGenerator spanningTreeGenerator = new SpanningTreeGenerator(queryGraph, labelDictionary, typeDictionary);
        spanningTreeGenerator.generateQueryGraph();
        GraphModelConverter graphModelConverter = new GraphModelConverter(queryGraph.nodes, labelDictionary, typeDictionary);
        return graphModelConverter.convert();
    }

    private GpuGraphModel convertData(DatabaseService databaseService, LabelDictionary labelDictionary, TypeDictionary typeDictionary) {
        ResourceIterable<Node> allNodes = databaseService.getAllNodes();
        GraphModelConverter graphModelConverter = new GraphModelConverter(allNodes, labelDictionary, typeDictionary);
        return graphModelConverter.convert();
    }
}
