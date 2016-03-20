package se.simonevertsson.gpu.query;

import se.simonevertsson.gpu.dictionary.LabelDictionary;
import se.simonevertsson.gpu.dictionary.TypeDictionary;
import se.simonevertsson.gpu.graph.GpuGraph;
import se.simonevertsson.runner.QueryGraph;

public class QueryContext {
    public LabelDictionary labelDictionary;
    public TypeDictionary typeDictionary;
    public QueryGraph queryGraph;
    public GpuGraph gpuData;
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