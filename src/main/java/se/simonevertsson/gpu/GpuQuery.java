package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.*;
import org.bridj.Pointer;
import org.neo4j.graphdb.Node;
import se.simonevertsson.CheckCandidates;
import se.simonevertsson.query.QueryGraph;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Arrays;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class GpuQuery {

    private final CLContext context;
    private final CLQueue queue;
    private CLBuffer<Integer> adjacency_indicies;
    private CLBuffer<Integer> labels;
    private CLBuffer<Integer> label_indicies;

    public GpuQuery() {
        this.context = JavaCL.createBestContext();
        this.queue = this.context.createDefaultQueue();
    }

    public void executeQuery(GpuGraphModel gpuData, GpuGraphModel gpuQuery, QueryGraph queryGraph) throws IOException {


        int n = gpuData.getNodeLabels().length;
        int[] prefixScanArray = new int[n];
        prefixScanArray[0] = 0;
        int[] globalSizes = new int[] { n };
        createDataBuffers(gpuData);
        int orderCounter = 0;

        for(Node queryNode : queryGraph.visitOrder) {
            int[] candidateArray = checkCandidates(gpuQuery, n, prefixScanArray, globalSizes, orderCounter, queryNode);
            System.out.println("Candidate array for query node " + queryNode.getId() + ": " + Arrays.toString(candidateArray));
            orderCounter++;
        }
    }

    private void createDataBuffers(GpuGraphModel gpuData) {

        IntBuffer adjacencyIndexBuffer = IntBuffer.wrap(gpuData.getAdjacencyIndicies());
        IntBuffer labelsBuffer = IntBuffer.wrap(gpuData.getNodeLabels());
        IntBuffer labelIndexBuffer = IntBuffer.wrap(gpuData.getLabelIndicies());

        this.adjacency_indicies = context.createIntBuffer(CLMem.Usage.Input, adjacencyIndexBuffer, false);
        this.labels = context.createIntBuffer(CLMem.Usage.Input, labelsBuffer, false);
        this.label_indicies = context.createIntBuffer(CLMem.Usage.Input, labelIndexBuffer, false);
    }

    private int[] checkCandidates(GpuGraphModel gpuQuery, int n, int[] prefixScanArray, int[] globalSizes, int orderCounter, Node queryNode) throws IOException {
        System.out.println("---------------------------");
        int queryLabelStartIndex = gpuQuery.getLabelIndicies()[orderCounter];
        int queryLabelEndIndex = gpuQuery.getLabelIndicies()[orderCounter+1];
        int query_vertex_label_count = queryLabelEndIndex - queryLabelStartIndex;
        int query_node_degree = queryNode.getDegree();
        int[] queryNodeLabels = Arrays.copyOfRange(gpuQuery.getNodeLabels(), queryLabelStartIndex, queryLabelEndIndex);


        System.out.println("Query node: " + queryNode.getId());
        System.out.println("Query node degree: " + query_node_degree);
        System.out.println("Query node labels: " + Arrays.toString(queryNodeLabels));


        // Create OpenCL input and output buffers
        CLBuffer<Integer>
                query_vertex_labels = this.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryNodeLabels), false),
                c_set = context.createIntBuffer(CLMem.Usage.Output, n);
        CheckCandidates kernels = new CheckCandidates(context);


        CLEvent checkCandidatesEvent = kernels.check_candidates(
                this.queue,
                this.adjacency_indicies,
                query_node_degree,
                this.labels,
                this.label_indicies,
                query_vertex_labels,
                query_vertex_label_count,
                c_set,
                n,
                globalSizes,
                null);

        Pointer<Integer> outPtr = c_set.read(this.queue, checkCandidatesEvent); // blocks until add_floats finished

        int[] candidateArray = gatherCandidateArray(n, prefixScanArray, outPtr);
        return candidateArray;
    }

    private int[] gatherCandidateArray(int n, int[] prefixScanArray, Pointer<Integer> outPtr) {
        int candidateCount = 0;
        if(outPtr.get(0) == 1) {
            candidateCount++;
        }

        for (int i = 1; i < n; i++) {
            prefixScanArray[i] = prefixScanArray[i-1] + outPtr.get(i-1);
            if(outPtr.get(i) == 1) {
                candidateCount++;
            }
        }

        int[] candidateArray = new int[candidateCount];

        for (int i = 0; i < n; i++) {
            if(outPtr.get(i) == 1) {
                candidateArray[prefixScanArray[i]] = i;
            }
        }
        return candidateArray;
    }
}
