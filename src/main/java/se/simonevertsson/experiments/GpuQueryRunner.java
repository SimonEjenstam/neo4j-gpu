package se.simonevertsson.experiments;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.gpu.*;
import se.simonevertsson.query.QueryGraph;
import se.simonevertsson.query.QueryGraphGenerator;

import java.io.IOException;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class GpuQueryRunner {

    public void runGpuQuery(DatabaseService databaseService) throws IOException {
        long tick, tock;

        /* Convert database data and query data to fit the GPU */
        tick = System.currentTimeMillis();
        LabelDictionary labelDictionary = new LabelDictionary();
        QueryGraph queryGraph = QueryGraphGenerator.generateMockQueryGraph();

        GpuGraphModel gpuData = convertData(databaseService, labelDictionary);
        GpuGraphModel gpuQuery = convertQuery(labelDictionary, queryGraph);
        tock = System.currentTimeMillis();

        System.out.println("GPU Data conversion runtime: " + (tock-tick) + "ms");
        System.out.println("------Query-----");
        System.out.println(gpuQuery.toString());
        System.out.println("------Data-----");
        System.out.println(gpuData.toString());

        /* Execute the query */
        tick = System.currentTimeMillis();
        GpuQuery gpuGraphQuery = new GpuQuery(gpuData, gpuQuery);
        gpuGraphQuery.executeQuery(queryGraph.visitOrder);
        tock = System.currentTimeMillis();

        System.out.println("GPU Query runtime: " + (tock - tick) + "ms");
    }

    private GpuGraphModel convertQuery(LabelDictionary labelDictionary, QueryGraph queryGraph) {
        SpanningTreeGenerator spanningTreeGenerator = new SpanningTreeGenerator(queryGraph, labelDictionary);
        spanningTreeGenerator.generateQueryGraph();
        GraphModelConverter graphModelConverter = new GraphModelConverter(queryGraph.visitOrder, labelDictionary);
        return graphModelConverter.convert();
    }

    private GpuGraphModel convertData(DatabaseService databaseService, LabelDictionary labelDictionary) {
        ResourceIterable<Node> allNodes = databaseService.getAllNodes();
        GraphModelConverter graphModelConverter = new GraphModelConverter(allNodes, labelDictionary);
        return graphModelConverter.convert();
    }
}
