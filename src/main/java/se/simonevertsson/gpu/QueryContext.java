package se.simonevertsson.gpu;

import se.simonevertsson.query.AliasDictionary;
import se.simonevertsson.query.QueryGraph;

public class QueryContext {
    LabelDictionary labelDictionary;
    TypeDictionary typeDictionary;
    public QueryGraph queryGraph;
    GpuGraphModel gpuData;
    public GpuGraphModel gpuQuery;
    public int queryNodeCount;
    public int dataNodeCount;

    public QueryContext(GpuGraphModel gpuData, GpuGraphModel gpuQuery, QueryGraph queryGraph, LabelDictionary labelDictionary, TypeDictionary typeDictionary) {
        this.gpuData = gpuData;
        this.gpuQuery = gpuQuery;
        this.queryGraph = queryGraph;
        this.dataNodeCount = gpuData.getRelationshipIndices().length - 1;
        this.queryNodeCount = gpuQuery.getRelationshipIndices().length - 1;
        this.labelDictionary = labelDictionary;
        this.typeDictionary = typeDictionary;
    }
}