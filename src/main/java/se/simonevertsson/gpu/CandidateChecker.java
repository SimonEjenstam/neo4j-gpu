package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLEvent;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import java.io.IOException;

public class CandidateChecker {

    private final QueryKernels queryKernels;
    private final QueryBuffers queryBuffers;
    private final DataBuffers dataBuffers;
    private final int dataNodeCount;
    private final QueryContext queryContext;

    public CandidateChecker(QueryContext queryContext, QueryKernels queryKernels, BufferContainer bufferContainer, int dataNodeCount) {
        this.queryContext = queryContext;
        this.queryKernels = queryKernels;
        this.queryBuffers = bufferContainer.queryBuffers;
        this.dataBuffers = bufferContainer.dataBuffers;
        this.dataNodeCount = dataNodeCount;
    }

    public void checkCandidates(GpuGraph gpuQuery, Node queryNode) throws IOException {
        int queryNodeId = this.queryContext.gpuQuery.getQueryIdDictionary().getQueryId(queryNode.getId());

        int queryLabelStartIndex = gpuQuery.getLabelIndices()[queryNodeId];
        int queryLabelEndIndex = gpuQuery.getLabelIndices()[queryNodeId + 1];
        int queryNodeDegree = queryNode.getDegree(Direction.OUTGOING);
        int[] globalSizes = new int[]{ this.dataNodeCount };


        CLEvent checkCandidatesEvent = this.queryKernels.checkCandidatesKernel.check_candidates(
                this.queryKernels.queue,
                this.queryBuffers.queryNodeLabelsBuffer,
                queryNodeId,
                queryLabelStartIndex,
                queryLabelEndIndex,
                queryNodeDegree,
                this.dataBuffers.dataLabelIndicesBuffer,
                this.dataBuffers.dataLabelsBuffer,
                this.dataBuffers.dataNodeRelationshipsBuffer,
                this.dataBuffers.dataRelationshipIndicesBuffer,
                this.queryBuffers.candidateIndicatorsBuffer,
                this.dataNodeCount,
                globalSizes,
                null);

        this.queryBuffers.candidateIndicatorsPointer =
                this.queryBuffers.candidateIndicatorsBuffer.read(this.queryKernels.queue, checkCandidatesEvent);
    }
}