package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;

public class CandidateInitializer {
    private final QueryContext queryContext;
    private final QueryKernels queryKernels;
    private final QueryBuffers queryBuffers;
    private final DataBuffers dataBuffers;



    public CandidateInitializer(QueryContext queryContext, QueryKernels queryKernels, BufferContainer bufferContainer) {
        this.queryContext = queryContext;
        this.queryKernels = queryKernels;
        this.queryBuffers = bufferContainer.queryBuffers;
        this.dataBuffers = bufferContainer.dataBuffers;
    }

    void candidateInitialization(ArrayList<Node> visitOrder) throws IOException {
        boolean initializedQueryNodes[] = new boolean[this.queryContext.queryNodeCount];
        for (Node queryNode : visitOrder) {
            if (!initializedQueryNodes[((int) queryNode.getId())]) {
                checkCandidates(this.queryContext.gpuQuery, queryNode);
            }
            int[] candidateArray = QueryUtils.gatherCandidateArray(
                    this.queryBuffers.candidateIndicatorsPointer,
                    this.queryContext.dataNodeCount,
                    (int) queryNode.getId());

            if (candidateArray.length > 0) {
                exploreCandidates((int) queryNode.getId(), candidateArray);
                for (Relationship adjacency : queryNode.getRelationships()) {
                    initializedQueryNodes[((int) adjacency.getEndNode().getId())] = true;
                }
            } else {
                throw new IllegalStateException("No candidates for query node " + queryNode.getId() + " were found.");
            }
        }
    }

    void exploreCandidates(int queryNodeId, int[] candidateArray) throws IOException {
        int queryNodeAdjacencyIndexStart = this.queryContext.gpuQuery.getAdjacencyIndices()[queryNodeId];
        int queryNodeAdjacencyIndexEnd = this.queryContext.gpuQuery.getAdjacencyIndices()[queryNodeId + 1];

        IntBuffer candidatesArrayBuffer = IntBuffer.wrap(candidateArray);
        CLBuffer<Integer> candidatesArray
                = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, candidatesArrayBuffer, true);
        int[] globalSizes = new int[]{candidateArray.length};

        CLEvent exploreCandidatesEvent = this.queryKernels.exploreCandidatesKernel.explore_candidates(
                this.queryKernels.queue,
                queryNodeId,
                this.queryBuffers.queryNodeAdjacenciesBuffer,
                this.queryBuffers.queryNodeAdjacencyIndicesBuffer,
                this.queryBuffers.queryNodeLabelsBuffer,
                this.queryBuffers.queryNodeLabelIndicesBuffer,
                queryNodeAdjacencyIndexStart,
                queryNodeAdjacencyIndexEnd,
                this.dataBuffers.dataAdjacencesBuffer,
                this.dataBuffers.dataAdjacencyIndicesBuffer,
                this.dataBuffers.dataLabelsBuffer,
                this.dataBuffers.dataLabelIndicesBuffer,
                candidatesArray,
                this.queryBuffers.candidateIndicatorsBuffer,
                this.queryContext.dataNodeCount,
                globalSizes,
                null
        );

        this.queryBuffers.candidateIndicatorsPointer =
                this.queryBuffers.candidateIndicatorsBuffer.read(this.queryKernels.queue, exploreCandidatesEvent);
    }

    public void checkCandidates(GpuGraphModel gpuQuery, Node queryNode) throws IOException {
        int queryLabelStartIndex = gpuQuery.getLabelIndicies()[((int) queryNode.getId())];
        int queryLabelEndIndex = gpuQuery.getLabelIndicies()[((int) queryNode.getId()) + 1];
        int queryNodeDegree = queryNode.getDegree();
        int[] globalSizes = new int[]{this.queryContext.dataNodeCount};


        CLEvent checkCandidatesEvent = this.queryKernels.checkCandidatesKernel.check_candidates(
                this.queryKernels.queue,
                this.queryBuffers.queryNodeLabelsBuffer,
                (int) queryNode.getId(),
                queryLabelStartIndex,
                queryLabelEndIndex,
                queryNodeDegree,
                this.dataBuffers.dataLabelIndicesBuffer,
                this.dataBuffers.dataLabelsBuffer,
                this.dataBuffers.dataAdjacencyIndicesBuffer,
                this.queryBuffers.candidateIndicatorsBuffer,
                this.queryContext.dataNodeCount,
                globalSizes,
                null);

        this.queryBuffers.candidateIndicatorsPointer =
                this.queryBuffers.candidateIndicatorsBuffer.read(this.queryKernels.queue, checkCandidatesEvent);
    }
}