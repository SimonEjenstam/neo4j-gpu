package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;

import java.nio.IntBuffer;

public class BufferContainerGenerator {

    public static BufferContainer generateBufferContainer(QueryContext queryContext, QueryKernels queryKernels) {
        DataBuffers dataBuffers = createDataBuffers(queryKernels, queryContext);
        QueryBuffers queryBuffers = createQueryBuffers(queryKernels, queryContext);
        return new BufferContainer(dataBuffers, queryBuffers);
    }

    private static DataBuffers createDataBuffers(QueryKernels queryKernels, QueryContext queryContext) {
        GpuGraphModel data = queryContext.gpuData;
        IntBuffer dataAdjacenciesBuffer = IntBuffer.wrap(data.getNodeAdjecencies());
        IntBuffer dataAdjacencyIndexBuffer = IntBuffer.wrap(data.getAdjacencyIndices());
        IntBuffer dataLabelsBuffer = IntBuffer.wrap(data.getNodeLabels());
        IntBuffer dataLabelIndexBuffer = IntBuffer.wrap(data.getLabelIndicies());

        DataBuffers dataBuffers = new DataBuffers();

        dataBuffers.dataAdjacencesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataAdjacenciesBuffer, true);
        dataBuffers.dataAdjacencyIndicesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataAdjacencyIndexBuffer, true);
        dataBuffers.dataLabelsBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataLabelsBuffer, true);
        dataBuffers.dataLabelIndicesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataLabelIndexBuffer, true);


        return dataBuffers;
    }


    private static QueryBuffers createQueryBuffers(QueryKernels queryKernels, QueryContext queryContext) {
        GpuGraphModel query = queryContext.gpuQuery;

        IntBuffer queryNodeAdjacenciesBuffer = IntBuffer.wrap(query.getNodeAdjecencies());
        IntBuffer queryNodeAdjacencyIndiciesBuffer = IntBuffer.wrap(query.getAdjacencyIndices());
        IntBuffer queryNodeLabelsBuffer = IntBuffer.wrap(query.getNodeLabels());
        IntBuffer queryNodeLabelIndiciesBuffer = IntBuffer.wrap(query.getLabelIndicies());

        boolean candidateIndicators[] = new boolean[queryContext.dataNodeCount * queryContext.queryNodeCount];

        QueryBuffers queryBuffers = new QueryBuffers();

        queryBuffers.queryNodeLabelsBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelsBuffer, true);
        queryBuffers.queryNodeLabelIndicesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelIndiciesBuffer, true);
        queryBuffers.queryNodeAdjacenciesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacenciesBuffer, true);
        queryBuffers.queryNodeAdjacencyIndicesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacencyIndiciesBuffer, true);

        queryBuffers.candidateIndicatorsBuffer = queryKernels.context.createBuffer(
            CLMem.Usage.Output,
            Pointer.pointerToBooleans(candidateIndicators), true);

        return queryBuffers;
    }
}