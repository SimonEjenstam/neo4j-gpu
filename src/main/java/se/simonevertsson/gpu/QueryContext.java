package se.simonevertsson.gpu;

import se.simonevertsson.query.QueryGraph;

public class QueryContext {
    LabelDictionary labelDictionary;
    TypeDictionary typeDictionary;
    public QueryGraph queryGraph;
    GpuGraph gpuData;
    public GpuGraph gpuQuery;
    public int queryNodeCount;
    public int dataNodeCount;
    public int queryRelationshipCount;

    public QueryContext(GpuGraph gpuData, GpuGraph gpuQuery, QueryGraph queryGraph, LabelDictionary labelDictionary, TypeDictionary typeDictionary) {
        this.gpuData = gpuData;
        this.gpuQuery = gpuQuery;
        this.queryGraph = queryGraph;
        this.dataNodeCount = gpuData.getRelationshipIndices().length - 1;
        this.queryNodeCount = gpuQuery.getRelationshipIndices().length - 1;
        this.queryRelationshipCount = queryGraph.relationships.size();
        this.labelDictionary = labelDictionary;
        this.typeDictionary = typeDictionary;
    }
}