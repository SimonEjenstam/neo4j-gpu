package se.simonevertsson.runner;

import org.neo4j.graphdb.Node;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.gpu.graph.GpuGraph;
import se.simonevertsson.gpu.graph.GpuGraphConverter;
import se.simonevertsson.gpu.query.*;
import se.simonevertsson.gpu.kernel.QueryKernels;
import se.simonevertsson.gpu.dictionary.LabelDictionary;
import se.simonevertsson.gpu.dictionary.TypeDictionary;

import java.io.IOException;
import java.util.List;

public class GpuQueryRunner {

  public QueryResult runGpuQuery(DatabaseService databaseService, QueryGraph queryGraph, QueryKernels queryKernels) throws IOException {
    long tick, tock;


    tick = System.currentTimeMillis();

    // Convert database data and runner data to fit the GPU
    LabelDictionary labelDictionary = new LabelDictionary();
    TypeDictionary typeDictionary = new TypeDictionary();
    GpuGraph gpuData = convertData(databaseService, labelDictionary, typeDictionary);
    GpuGraph gpuQuery = convertQuery(queryGraph, labelDictionary, typeDictionary);
    tock = System.currentTimeMillis();
    long conversionExecutionTime = tock - tick;

    tick = System.currentTimeMillis();

    // Execute the runner
    QueryContext queryContext = new QueryContext(gpuData, gpuQuery, queryGraph, labelDictionary, typeDictionary);
    GpuQuery gpuGraphQuery = new GpuQuery(queryContext, queryKernels);
    List<QuerySolution> solutions = gpuGraphQuery.executeQuery(queryGraph.getSpanningTree().getVisitOrder());

    tock = System.currentTimeMillis();
    long queryExecutionTime = tock - tick;

    return new QueryResult(solutions, conversionExecutionTime, queryExecutionTime);
  }

  public List<QuerySolution> runGpuQuery(QueryGraph dataGraph, QueryGraph queryGraph) throws IOException {
    long tick, tock;


    tick = System.currentTimeMillis();

    // Convert database data and runner data to fit the GPU
    LabelDictionary labelDictionary = new LabelDictionary();
    TypeDictionary typeDictionary = new TypeDictionary();
    GpuGraph gpuData = convertData(dataGraph, labelDictionary, typeDictionary);
    GpuGraph gpuQuery = convertQuery(queryGraph, labelDictionary, typeDictionary);

    tock = System.currentTimeMillis();
    System.out.println("GPU Data conversion runtime: " + (tock - tick) + "ms");

    tick = System.currentTimeMillis();

    // Execute the runner
    QueryContext queryContext = new QueryContext(gpuData, gpuQuery, queryGraph, labelDictionary, typeDictionary);
    GpuQuery gpuGraphQuery = new GpuQuery(queryContext);
    List<QuerySolution> results = gpuGraphQuery.executeQuery(queryGraph.getSpanningTree().getVisitOrder());

    tock = System.currentTimeMillis();
    System.out.println("GPU Query runtime: " + (tock - tick) + "ms");

    return results;
  }

  private GpuGraph convertQuery(QueryGraph queryGraph, LabelDictionary labelDictionary, TypeDictionary typeDictionary) {
    SpanningTreeGenerator spanningTreeGenerator = new SpanningTreeGenerator();
    SpanningTree spanningTree = spanningTreeGenerator.generate(queryGraph);
    queryGraph.setSpanningTree(spanningTree);

    GpuGraphConverter gpuGraphConverter = new GpuGraphConverter(queryGraph.nodes, labelDictionary, typeDictionary);
    return gpuGraphConverter.convert();
  }

  private GpuGraph convertData(DatabaseService databaseService, LabelDictionary labelDictionary, TypeDictionary typeDictionary) {
    List<Node> allNodes = databaseService.getAllNodes();
    GpuGraphConverter gpuGraphConverter = new GpuGraphConverter(allNodes, labelDictionary, typeDictionary);
    return gpuGraphConverter.convert();
  }

  private GpuGraph convertData(QueryGraph dataGraph, LabelDictionary labelDictionary, TypeDictionary typeDictionary) {
    List<Node> allNodes = dataGraph.nodes;
    GpuGraphConverter gpuGraphConverter = new GpuGraphConverter(allNodes, labelDictionary, typeDictionary);
    return gpuGraphConverter.convert();
  }


}
