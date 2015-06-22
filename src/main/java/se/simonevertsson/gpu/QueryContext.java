package se.simonevertsson.gpu;

import se.simonevertsson.query.QueryGraph;

public class QueryContext {
    QueryGraph queryGraph;
    GpuGraphModel gpuData;
    GpuGraphModel gpuQuery;
    int queryNodeCount;
    int dataNodeCount;

    public QueryContext(GpuGraphModel gpuData, GpuGraphModel gpuQuery, QueryGraph queryGraph) {
        this.gpuData = gpuData;
        this.gpuQuery = gpuQuery;
        this.queryGraph = queryGraph;
        this.dataNodeCount = gpuData.getAdjacencyIndices().length - 1;
        this.queryNodeCount = gpuQuery.getNodeAdjecencies().length - 1;
    }
}