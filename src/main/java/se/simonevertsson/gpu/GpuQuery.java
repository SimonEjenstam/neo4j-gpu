package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.*;
import org.bridj.Pointer;
import org.neo4j.graphdb.Node;
import se.simonevertsson.CheckCandidates;
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
    private CLBuffer<Integer> dataAdjacencyIndicies;
    private CLBuffer<Integer> dataLabels;
    private CLBuffer<Integer> dataLabelIndicies;
    private CLBuffer<Boolean> candidateIndicators;
    private CLBuffer<Long> dataAdjacencies;
    private CLBuffer<Integer> queryNodeLabels;
    private CLBuffer<Integer> queryNodeLabelIndicies;
    private CLBuffer<Long> queryNodeAdjacencies;
    private CLBuffer<Integer> queryNodeAdjacencyIndicies;

    public GpuQuery(GpuGraphModel gpuData, GpuGraphModel gpuQuery) {
        this.gpuData = gpuData;
        this.gpuQuery = gpuQuery;
        this.context = JavaCL.createBestContext();
        this.queue = this.context.createDefaultQueue();
    }

    public void executeQuery(ArrayList<Node> visitOrder) throws IOException {


        int dataNodeCount = gpuData.getNodeLabels().length;
        int queryNodeCount = gpuQuery.getNodeLabels().length;

        int[] globalSizes = new int[] { dataNodeCount };
        createDataBuffers(dataNodeCount, queryNodeCount);

        int orderCounter = 0;
        for(Node queryNode : visitOrder) {
            int[] candidateArray = checkCandidates(gpuQuery, dataNodeCount, globalSizes, queryNode);
            System.out.println("Candidate array for query node " + queryNode.getId() + ": " + Arrays.toString(candidateArray));
            orderCounter++;
        }
    }

    private void createDataBuffers(int dataNodeCount, int queryNodeCount) {
        LongBuffer queryNodeAdjacenciesBuffer = LongBuffer.wrap(gpuQuery.getNodeAdjecencies());
        IntBuffer queryNodeAdjacencyIndiciesBuffer = IntBuffer.wrap(gpuQuery.getAdjacencyIndicies());
        IntBuffer queryNodeLabelsBuffer = IntBuffer.wrap(gpuQuery.getNodeLabels());
        IntBuffer queryNodeLabelIndiciesBuffer = IntBuffer.wrap(gpuQuery.getLabelIndicies());


        LongBuffer dataAdjacenciesBuffer = LongBuffer.wrap(gpuData.getNodeAdjecencies());
        IntBuffer dataAdjacencyIndexBuffer = IntBuffer.wrap(gpuData.getAdjacencyIndicies());
        IntBuffer dataLabelsBuffer = IntBuffer.wrap(gpuData.getNodeLabels());
        IntBuffer dataLabelIndexBuffer = IntBuffer.wrap(gpuData.getLabelIndicies());


        this.queryNodeLabels = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelsBuffer, true);
        this.queryNodeLabelIndicies = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelIndiciesBuffer, true);
        this.queryNodeAdjacencies = this.context.createLongBuffer(CLMem.Usage.Input, queryNodeAdjacenciesBuffer, true);
        this.queryNodeAdjacencyIndicies = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacencyIndiciesBuffer, true);

        this.dataAdjacencies = this.context.createLongBuffer(CLMem.Usage.Input, dataAdjacenciesBuffer, true);
        this.dataAdjacencyIndicies = this.context.createIntBuffer(CLMem.Usage.Input, dataAdjacencyIndexBuffer, true);
        this.dataLabels = context.createIntBuffer(CLMem.Usage.Input, dataLabelsBuffer, true);
        this.dataLabelIndicies = context.createIntBuffer(CLMem.Usage.Input, dataLabelIndexBuffer, true);


        this.candidateIndicators = context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToBooleans(new boolean[dataNodeCount * queryNodeCount]));
    }

    public int[] checkCandidates(GpuGraphModel gpuQuery, int dataNodeCount, int[] globalSizes, Node queryNode) throws IOException {
        System.out.println("---------------------------");
        int queryLabelStartIndex = gpuQuery.getLabelIndicies()[((int) queryNode.getId())];
        int queryLabelEndIndex = gpuQuery.getLabelIndicies()[((int) queryNode.getId())+1];
        int queryNodeDegree = queryNode.getDegree();
        int[] queryNodeLabels = Arrays.copyOfRange(gpuQuery.getNodeLabels(), queryLabelStartIndex, queryLabelEndIndex);


        System.out.println("Query node: " + queryNode.getId());
        System.out.println("Query node degree: " + queryNodeDegree);
        System.out.println("Query node dataLabels: " + Arrays.toString(queryNodeLabels));

        CheckCandidates kernels = new CheckCandidates(context);
        CLEvent checkCandidatesEvent = kernels.check_candidates(
                this.queue,
                this.queryNodeLabels,
                (int)queryNode.getId(),
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

        Pointer<Boolean> outPtr = candidateIndicators.read(this.queue, checkCandidatesEvent); // blocks until add_floats finished
        StringBuilder builder = new StringBuilder();
        int j = 1;
        for(boolean candidate : outPtr)  {
            builder.append(candidate + ", ");
            if(j % dataNodeCount == 0) {
                builder.append("\n");
            }
            j++;
        }
        System.out.println(builder.toString());

        int[] candidateArray = gatherCandidateArray((int)queryNode.getId(), outPtr, dataNodeCount);
        return candidateArray;
    }

    public int[] gatherCandidateArray(int nodeId, Pointer<Boolean> outPtr, int dataNodeCount) {
        int[] prefixScanArray = new int[dataNodeCount];
        int candidateCount = 0;
        int offset = dataNodeCount * nodeId;
        if(outPtr.get(offset + 0)) {
            candidateCount++;
        }

        for (int i = 1; i < dataNodeCount; i++) {
            int nextElement = outPtr.get(offset + i-1) ? 1 : 0;
            prefixScanArray[i] = prefixScanArray[i-1] + nextElement;
            if(outPtr.get(offset + i)) {
                candidateCount++;
            }
        }

        int[] candidateArray = new int[candidateCount];

        for (int i = 0; i < dataNodeCount; i++) {
            if(outPtr.get(offset + i)) {
                candidateArray[prefixScanArray[i]] = i;
            }
        }
        return candidateArray;
    }
}
