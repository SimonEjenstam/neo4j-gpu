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
        IntBuffer dataRelationshipsBuffer = IntBuffer.wrap(data.getNodeRelationships());
        IntBuffer dataRelationshipTypesBuffer = IntBuffer.wrap(data.getRelationshipTypes());
        IntBuffer dataRelationshipIndicesBuffer = IntBuffer.wrap(data.getRelationshipIndices());
        IntBuffer dataLabelsBuffer = IntBuffer.wrap(data.getNodeLabels());
        IntBuffer dataLabelIndexBuffer = IntBuffer.wrap(data.getLabelIndices());

        DataBuffers dataBuffers = new DataBuffers();

        dataBuffers.dataNodeRelationshipsBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataRelationshipsBuffer, true);
        dataBuffers.dataRelationshipTypesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataRelationshipTypesBuffer, true);
        dataBuffers.dataRelationshipIndicesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataRelationshipIndicesBuffer, true);
        dataBuffers.dataLabelsBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataLabelsBuffer, true);
        dataBuffers.dataLabelIndicesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataLabelIndexBuffer, true);


        return dataBuffers;
    }


    private static QueryBuffers createQueryBuffers(QueryKernels queryKernels, QueryContext queryContext) {
        GpuGraphModel query = queryContext.gpuQuery;

        IntBuffer queryNodeRelationshipsBuffer = IntBuffer.wrap(query.getNodeRelationships());
        IntBuffer queryRelationshipTypesBuffer = IntBuffer.wrap(query.getRelationshipTypes());
        IntBuffer queryRelationshipIndicesBuffer = IntBuffer.wrap(query.getRelationshipIndices());
        IntBuffer queryNodeLabelsBuffer = IntBuffer.wrap(query.getNodeLabels());
        IntBuffer queryNodeLabelIndicesBuffer = IntBuffer.wrap(query.getLabelIndices());

        boolean candidateIndicators[] = new boolean[queryContext.dataNodeCount * queryContext.queryNodeCount];

        QueryBuffers queryBuffers = new QueryBuffers();

        queryBuffers.queryNodeLabelsBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelsBuffer, true);
        queryBuffers.queryNodeLabelIndicesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelIndicesBuffer, true);
        queryBuffers.queryNodeRelationshipsBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeRelationshipsBuffer, true);
        queryBuffers.queryRelationshipTypesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryRelationshipTypesBuffer, true);
        queryBuffers.queryRelationshipIndicesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryRelationshipIndicesBuffer, true);

        queryBuffers.candidateIndicatorsBuffer = queryKernels.context.createBuffer(
            CLMem.Usage.Output,
            Pointer.pointerToBooleans(candidateIndicators), true);

        return queryBuffers;
    }
}