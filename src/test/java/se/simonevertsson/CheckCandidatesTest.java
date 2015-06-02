package se.simonevertsson;

import com.nativelibs4java.opencl.*;
import junit.framework.TestCase;
import org.bridj.Pointer;

import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

/**
 * Created by simon.evertsson on 2015-05-18.
 */
public class CheckCandidatesTest extends TestCase {

    long[] queryAdjacencies = {
            1,
            2,
            2,
            3,
            3
    };

    int[] queryAdjacencyIndicies = {
            0,
            2,
            4,
            5,
            5
    };

    int[] queryLabels = {
            1,
            2,
            1,
            3
    };

    int[] queryLabelIndicies = {
            0,
            1,
            2,
            3,
            4
    };

    long[] dataAdjacencies = {
            1,
            2,
            2,
            3,
            3
    };

    int[] dataAdjacencyIndicies = {
            0,
            2,
            4,
            5,
            5
    };

    int[] dataLabels = {
            1,
            2,
            1,
            3
    };

    int[] dataLabelIndicies = {
            0,
            1,
            2,
            3,
            4
    };

    int dataNodeCount = 4;

    int queryNodeCount = 4;

    private CLContext context;
    private CLQueue queue;

    private CLBuffer<Long> q_adjacencies;
    private CLBuffer<Integer> q_adjacency_indicies;
    private CLBuffer<Integer> q_labels;
    private CLBuffer<Integer> q_label_indicies;

    private CLBuffer<Long> d_adjacencies;
    private CLBuffer<Integer> d_adjacency_indicies;
    private CLBuffer<Integer> d_labels;
    private CLBuffer<Integer> d_label_indicies;

    public void setUp() {
        this.context = JavaCL.createBestContext();
        this.queue = context.createDefaultQueue();



        // Create OpenCL input and output buffers
        this.q_adjacencies = context.createLongBuffer(CLMem.Usage.Input, LongBuffer.wrap(queryAdjacencies), true);
        this.q_adjacency_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryAdjacencyIndicies), true);
        this.q_labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryLabels), true);
        this.q_label_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryLabelIndicies), true);

        this.d_adjacencies = context.createLongBuffer(CLMem.Usage.Input, LongBuffer.wrap(dataAdjacencies), true);
        this.d_adjacency_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataAdjacencyIndicies), true);
        this.d_labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabels), true);
        this.d_label_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabelIndicies), true);

    }

    public void testReturnsTrueForCorrectCandidate() throws Exception {
        // Given


        boolean[] candidateIndicators = {
            false, false, false, false,
            false, false, false, false,
            false, false, false, false,
            false, false, false, false,
        };


        boolean[] expectedCandidateSet = {
                false, false, false, false,
                false, true, false, false,
                false, false, false, false,
                false, false, false, false,
        };


        // Create OpenCL input and output buffers
        CLBuffer<Integer>
                labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabels), true),
                label_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabelIndicies), true),
                adjacency_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataAdjacencyIndicies), true),
                query_vertex_labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryLabels), true);
        CLBuffer<Boolean>
                c_set = context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(candidateIndicators));


        CheckCandidates kernels = new CheckCandidates(context);
        int[] globalSizes = new int[] { dataNodeCount };

        // When
        CLEvent checkCandidatesEvent = kernels.check_candidates(
                queue,
                query_vertex_labels,
                1,
                1,
                2,
                2,
                label_indicies,
                labels,
                adjacency_indicies,
                c_set,
                dataNodeCount,
                globalSizes,
                null);

        Pointer<Boolean> outPtr = c_set.read(queue, checkCandidatesEvent); // blocks until add_floats finished

        // Then
        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateSet[i], (boolean) outPtr.get(i));
        }
    }


    public void testReturnsTrueForCorrectCandidate2() throws Exception {
        // Given
        boolean[] candidateIndicators = {
                false, false, false, false,
                false, false, false, false,
                false, false, false, false,
                false, false, false, false,
        };


        boolean[] expectedCandidateSet = {
                true, false, false, false,
                false, false, false, false,
                false, false, false, false,
                false, false, false, false,
        };



        // Create OpenCL input and output buffers
        CLBuffer<Integer>
                labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabels), true),
                label_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabelIndicies), true),
                adjacency_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataAdjacencyIndicies), true),
                query_vertex_labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryLabels), true);
        CLBuffer<Boolean>
                 c_set = context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(candidateIndicators));


        CheckCandidates kernels = new CheckCandidates(context);
        int[] globalSizes = new int[] { dataNodeCount };

        // When
        CLEvent checkCandidatesEvent = kernels.check_candidates(
                queue,
                query_vertex_labels,
                0,
                0,
                1,
                2,
                label_indicies,
                labels,
                adjacency_indicies,
                c_set,
                dataNodeCount,
                globalSizes,
                null);

        Pointer<Boolean> outPtr = c_set.read(queue, checkCandidatesEvent); // blocks until finished

        // Then
        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateSet[i], (boolean) outPtr.get(i));
        }
    }
}
