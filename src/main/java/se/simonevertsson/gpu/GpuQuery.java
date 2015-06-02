package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.*;
import org.bridj.Pointer;
import org.neo4j.graphdb.Node;
import se.simonevertsson.CheckCandidates;
import se.simonevertsson.ExploreCandidates;
import se.simonevertsson.query.QueryGraph;

import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class GpuQuery {

    private final CLContext context;
    private final CLQueue queue;
    private final GpuGraphModel gpuData;
    private final GpuGraphModel gpuQuery;
    private final int queryNodeCount;
    private final int dataNodeCount;
    private CLBuffer<Integer> dataAdjacencyIndicies;
    private CLBuffer<Integer> dataLabels;
    private CLBuffer<Integer> dataLabelIndicies;
    private CLBuffer<Boolean> candidateIndicators;
    private CLBuffer<Integer> dataAdjacencies;
    private CLBuffer<Integer> queryNodeLabels;
    private CLBuffer<Integer> queryNodeLabelIndicies;
    private CLBuffer<Integer> queryNodeAdjacencies;
    private CLBuffer<Integer> queryNodeAdjacencyIndicies;
    private boolean[] initializedQueryNode;
    private Pointer<Boolean> candidateIndicatorsPointer;


    public GpuQuery(GpuGraphModel gpuData, GpuGraphModel gpuQuery) {
        this.gpuData = gpuData;
        this.gpuQuery = gpuQuery;
        this.context = JavaCL.createBestContext();
        this.queue = this.context.createDefaultQueue();
        this.dataNodeCount = gpuData.getNodeLabels().length;
        this.queryNodeCount = gpuQuery.getNodeLabels().length;
        this.initializedQueryNode = new boolean[queryNodeCount];
    }

    public void executeQuery(ArrayList<Node> visitOrder) throws IOException {

        createDataBuffers(dataNodeCount, queryNodeCount);

        for(Node queryNode : visitOrder) {
            if(!initializedQueryNode[((int) queryNode.getId())]) {
                checkCandidates(gpuQuery, queryNode);
                printCandidateIndicatorMatrix();
            }
            int[] candidateArray = gatherCandidateArray((int) queryNode.getId());
            System.out.println("Candidate array for query node " + queryNode.getId() + ": " + Arrays.toString(candidateArray));
            exploreCandidates((int) queryNode.getId(), candidateArray);
            printCandidateIndicatorMatrix();
        }
    }

    private void printCandidateIndicatorMatrix() {
        StringBuilder builder = new StringBuilder();
        int j = 1;
        for(boolean candidate : candidateIndicatorsPointer)  {
            builder.append(candidate + ", ");
            if(j % dataNodeCount == 0) {
                builder.append("\n");
            }
            j++;
        }
        System.out.println(builder.toString());
    }

    private void exploreCandidates(int queryNodeId, int[] candidateArray) throws IOException {
        int queryNodeAdjacencyIndexStart = gpuQuery.getAdjacencyIndicies()[queryNodeId];
        int queryNodeAdjacencyIndexEnd = gpuQuery.getAdjacencyIndicies()[queryNodeId + 1];

        IntBuffer candidatesArrayBuffer = IntBuffer.wrap(candidateArray);
        CLBuffer<Integer> candidatesArray
                = this.context.createIntBuffer(CLMem.Usage.Input, candidatesArrayBuffer, true);
        int[] globalSizes = new int[] { candidateArray.length };

        ExploreCandidates kernel = new ExploreCandidates(context);
        CLEvent exploreCandidatesEvent = kernel.explore_candidates(
            queue,
            queryNodeId,
            queryNodeAdjacencies,
            queryNodeAdjacencyIndicies,
            queryNodeLabels,
            queryNodeLabelIndicies,
            queryNodeAdjacencyIndexStart,
            queryNodeAdjacencyIndexEnd,
            dataAdjacencies,
            dataAdjacencyIndicies,
            dataLabels,
            dataLabelIndicies,
            candidatesArray,
            candidateIndicators,
            this.dataNodeCount,
            globalSizes,
            null
        );

        this.candidateIndicatorsPointer = candidateIndicators.read(this.queue, exploreCandidatesEvent); // blocks until add_floats finished;
    }

    private void createDataBuffers(int dataNodeCount, int queryNodeCount) {
        IntBuffer queryNodeAdjacenciesBuffer = IntBuffer.wrap(gpuQuery.getNodeAdjecencies());
        IntBuffer queryNodeAdjacencyIndiciesBuffer = IntBuffer.wrap(gpuQuery.getAdjacencyIndicies());
        IntBuffer queryNodeLabelsBuffer = IntBuffer.wrap(gpuQuery.getNodeLabels());
        IntBuffer queryNodeLabelIndiciesBuffer = IntBuffer.wrap(gpuQuery.getLabelIndicies());


        IntBuffer dataAdjacenciesBuffer = IntBuffer.wrap(gpuData.getNodeAdjecencies());
        IntBuffer dataAdjacencyIndexBuffer = IntBuffer.wrap(gpuData.getAdjacencyIndicies());
        IntBuffer dataLabelsBuffer = IntBuffer.wrap(gpuData.getNodeLabels());
        IntBuffer dataLabelIndexBuffer = IntBuffer.wrap(gpuData.getLabelIndicies());


        this.queryNodeLabels = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelsBuffer, true);
        this.queryNodeLabelIndicies = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelIndiciesBuffer, true);
        this.queryNodeAdjacencies = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacenciesBuffer, true);
        this.queryNodeAdjacencyIndicies = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacencyIndiciesBuffer, true);

        this.dataAdjacencies = this.context.createIntBuffer(CLMem.Usage.Input, dataAdjacenciesBuffer, true);
        this.dataAdjacencyIndicies = this.context.createIntBuffer(CLMem.Usage.Input, dataAdjacencyIndexBuffer, true);
        this.dataLabels = context.createIntBuffer(CLMem.Usage.Input, dataLabelsBuffer, true);
        this.dataLabelIndicies = context.createIntBuffer(CLMem.Usage.Input, dataLabelIndexBuffer, true);


        this.candidateIndicators = context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToBooleans(new boolean[dataNodeCount * queryNodeCount]));
    }

    public void checkCandidates(GpuGraphModel gpuQuery, Node queryNode) throws IOException {
        System.out.println("---------------------------");
        int queryLabelStartIndex = gpuQuery.getLabelIndicies()[((int) queryNode.getId())];
        int queryLabelEndIndex = gpuQuery.getLabelIndicies()[((int) queryNode.getId())+1];
        int queryNodeDegree = queryNode.getDegree();
        int[] queryNodeLabels = Arrays.copyOfRange(gpuQuery.getNodeLabels(), queryLabelStartIndex, queryLabelEndIndex);
        int[] globalSizes = new int[] { dataNodeCount };


        System.out.println("Query node: " + queryNode.getId());
        System.out.println("Query node degree: " + queryNodeDegree);
        System.out.println("Query node dataLabels: " + Arrays.toString(queryNodeLabels));

        CheckCandidates kernels = new CheckCandidates(context);
        CLEvent checkCandidatesEvent = kernels.check_candidates(
                this.queue,
                this.queryNodeLabels,
                (int) queryNode.getId(),
                queryLabelStartIndex,
                queryLabelEndIndex,
                queryNodeDegree,
                this.dataLabelIndicies,
                this.dataLabels,
                this.dataAdjacencyIndicies,
                candidateIndicators,
                dataNodeCount,
                globalSizes,
                null);

        this.candidateIndicatorsPointer = candidateIndicators.read(this.queue, checkCandidatesEvent); // blocks until add_floats finished;
    }

    public int[] gatherCandidateArray(int nodeId) {
        int[] prefixScanArray = new int[dataNodeCount];
        int candidateCount = 0;
        int offset = dataNodeCount * nodeId;
        if(candidateIndicatorsPointer.get(offset + 0)) {
            candidateCount++;
        }

        for (int i = 1; i < dataNodeCount; i++) {
            int nextElement = candidateIndicatorsPointer.get(offset + i-1) ? 1 : 0;
            prefixScanArray[i] = prefixScanArray[i-1] + nextElement;
            if(candidateIndicatorsPointer.get(offset + i)) {
                candidateCount++;
            }
        }

        int[] candidateArray = new int[candidateCount];

        for (int i = 0; i < dataNodeCount; i++) {
            if(candidateIndicatorsPointer.get(offset + i)) {
                candidateArray[prefixScanArray[i]] = i;
            }
        }
        return candidateArray;
    }
}
