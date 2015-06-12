package se.simonevertsson;

import com.nativelibs4java.opencl.*;
import junit.framework.TestCase;
import org.bridj.Pointer;

import java.nio.IntBuffer;

/**
 * Created by simon.evertsson on 2015-05-18.
 */
public class SearchEdgeCandidatesTest extends TestCase {

    int[] queryAdjacencies = {
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

    int[] dataAdjacencies = {
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

    private CLBuffer<Integer> q_adjacencies;
    private CLBuffer<Integer> q_adjacency_indicies;
    private CLBuffer<Integer> q_labels;
    private CLBuffer<Integer> q_label_indicies;

    private CLBuffer<Integer> d_adjacencies;
    private CLBuffer<Integer> d_adjacency_indicies;
    private CLBuffer<Integer> d_labels;
    private CLBuffer<Integer> d_label_indicies;

    public void setUp() {
        this.context = JavaCL.createBestContext();
        this.queue = context.createDefaultQueue();


        // Create OpenCL input and output buffers
        this.q_adjacencies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryAdjacencies), true);
        this.q_adjacency_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryAdjacencyIndicies), true);
        this.q_labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryLabels), true);
        this.q_label_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryLabelIndicies), true);

        this.d_adjacencies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataAdjacencies), true);
        this.d_adjacency_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataAdjacencyIndicies), true);
        this.d_labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabels), true);
        this.d_label_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabelIndicies), true);

    }

    public void testReturnsTrueForCorrectCandidates1() throws Exception {
        // Given
        int[] candidateArray = new int[]{
                0
        };

        boolean[] candidateIndicators = {
                true, false, false, false,
                false, true, false, false,
                true, false, true, false,
                false, false, false, true,
        };

        int[] candidateEdgeEndNodeIndicies = {
                0
        };


        int[] expectedEdgeEndNodes = {
                1
        };

        CLBuffer<Boolean>
                candidate_indicators = context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(candidateIndicators));
        CLBuffer<Integer>
                candidates_array = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateArray), true);
        CLBuffer<Integer>
                candidatesEdgeEndNodes = context.createIntBuffer(CLMem.Usage.Output, 1);
        CLBuffer<Integer>
                candidatesEdgeEndNodesIndicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateEdgeEndNodeIndicies), true);


        SearchEdgeCandidates kernel = new SearchEdgeCandidates(context);
        int[] globalSizes = new int[]{candidateArray.length};

        // When
        CLEvent searchEdgeCandidatesEvent = kernel.count_edge_candidates(
                this.queue,
                0,
                1,
                this.d_adjacencies,
                this.d_adjacency_indicies,
                candidatesEdgeEndNodes,
                candidatesEdgeEndNodesIndicies,
                candidates_array,
                candidate_indicators,
                this.dataNodeCount,
                globalSizes,
                null
        );


        Pointer<Integer> outPtr = candidatesEdgeEndNodes.read(queue, searchEdgeCandidatesEvent); // blocks until add_floats finished

        // Then
        for (int i = 0; i < candidateArray.length; i++) {
            assertEquals(expectedEdgeEndNodes[i], (int) outPtr.get(i));
        }
    }

    public void testReturnsTrueForCorrectCandidates3() throws Exception {
        // Given
        int[] candidateArray = new int[]{
                0
        };

        boolean[] candidateIndicators = {
                true, false, false, false,
                false, true, false, false,
                true, false, true, false,
                false, false, false, true,
        };

        int[] candidateEdgeEndNodeIndicies = {
                0
        };


        int[] expectedEdgeEndNodes = {
                2
        };

        CLBuffer<Boolean>
                candidate_indicators = context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(candidateIndicators));
        CLBuffer<Integer>
                candidates_array = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateArray), true);
        CLBuffer<Integer>
                candidatesEdgeEndNodes = context.createIntBuffer(CLMem.Usage.Output, 1);
        CLBuffer<Integer>
                candidatesEdgeEndNodesIndicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateEdgeEndNodeIndicies), true);


        SearchEdgeCandidates kernel = new SearchEdgeCandidates(context);
        int[] globalSizes = new int[]{candidateArray.length};

        // When
        CLEvent searchEdgeCandidatesEvent = kernel.count_edge_candidates(
                this.queue,
                0,
                2,
                this.d_adjacencies,
                this.d_adjacency_indicies,
                candidatesEdgeEndNodes,
                candidatesEdgeEndNodesIndicies,
                candidates_array,
                candidate_indicators,
                this.dataNodeCount,
                globalSizes,
                null
        );


        Pointer<Integer> outPtr = candidatesEdgeEndNodes.read(queue, searchEdgeCandidatesEvent); // blocks until add_floats finished


        // Then
        for (int i = 0; i < candidateArray.length; i++) {
            assertEquals(expectedEdgeEndNodes[i], (int) outPtr.get(i));
        }
    }

    public void testReturnsTrueForCorrectCandidates2() throws Exception {
        // Given
        int[] candidateArray = new int[]{
                1
        };

        boolean[] candidateIndicators = {
                true, false, false, false,
                false, true, false, false,
                true, false, true, false,
                false, false, false, true,
        };

        int[] candidateEdgeEndNodeIndicies = {
                0
        };


        int[] expectedEdgeEndNodes = {
                2
        };

        CLBuffer<Boolean>
                candidate_indicators = context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(candidateIndicators));
        CLBuffer<Integer>
                candidates_array = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateArray), true);
        CLBuffer<Integer>
                candidatesEdgeEndNodes = context.createIntBuffer(CLMem.Usage.Output, 1);
        CLBuffer<Integer>
                candidatesEdgeEndNodesIndicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateEdgeEndNodeIndicies), true);


        SearchEdgeCandidates kernel = new SearchEdgeCandidates(context);
        int[] globalSizes = new int[]{candidateArray.length};

        // When
        CLEvent searchEdgeCandidatesEvent = kernel.count_edge_candidates(
                this.queue,
                1,
                2,
                this.d_adjacencies,
                this.d_adjacency_indicies,
                candidatesEdgeEndNodes,
                candidatesEdgeEndNodesIndicies,
                candidates_array,
                candidate_indicators,
                this.dataNodeCount,
                globalSizes,
                null
        );


        Pointer<Integer> outPtr = candidatesEdgeEndNodes.read(queue, searchEdgeCandidatesEvent); // blocks until add_floats finished

        // Then
        for (int i = 0; i < candidateArray.length; i++) {
            assertEquals(expectedEdgeEndNodes[i], (int) outPtr.get(i));
        }
    }

    public void testReturnsTrueForCorrectCandidates4() throws Exception {
        // Given
        int[] candidateArray = new int[]{
                1
        };

        boolean[] candidateIndicators = {
                true, false, false, false,
                false, true, false, false,
                true, false, true, false,
                false, false, false, true,
        };

        int[] candidateEdgeEndNodeIndicies = {
                0
        };


        int[] expectedEdgeEndNodes = {
                3
        };

        CLBuffer<Boolean>
                candidate_indicators = context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(candidateIndicators));
        CLBuffer<Integer>
                candidates_array = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateArray), true);
        CLBuffer<Integer>
                candidatesEdgeEndNodes = context.createIntBuffer(CLMem.Usage.Output, 1);
        CLBuffer<Integer>
                candidatesEdgeEndNodesIndicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateEdgeEndNodeIndicies), true);


        SearchEdgeCandidates kernel = new SearchEdgeCandidates(context);
        int[] globalSizes = new int[]{candidateArray.length};

        // When
        CLEvent searchEdgeCandidatesEvent = kernel.count_edge_candidates(
                this.queue,
                1,
                3,
                this.d_adjacencies,
                this.d_adjacency_indicies,
                candidatesEdgeEndNodes,
                candidatesEdgeEndNodesIndicies,
                candidates_array,
                candidate_indicators,
                this.dataNodeCount,
                globalSizes,
                null
        );


        Pointer<Integer> outPtr = candidatesEdgeEndNodes.read(queue, searchEdgeCandidatesEvent); // blocks until add_floats finished

        // Then
        for (int i = 0; i < candidateArray.length; i++) {
            assertEquals(expectedEdgeEndNodes[i], (int) outPtr.get(i));
        }
    }

    public void testReturnsTrueForCorrectCandidates5() throws Exception {
        // Given
        int[] candidateArray = new int[]{
                2
        };

        boolean[] candidateIndicators = {
                true, false, false, false,
                false, true, false, false,
                true, false, true, false,
                false, false, false, true,
        };

        int[] candidateEdgeEndNodeIndicies = {
                0
        };


        int[] expectedEdgeEndNodes = {
                3
        };

        CLBuffer<Boolean>
                candidate_indicators = context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(candidateIndicators));
        CLBuffer<Integer>
                candidates_array = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateArray), true);
        CLBuffer<Integer>
                candidatesEdgeEndNodes = context.createIntBuffer(CLMem.Usage.Output, 1);
        CLBuffer<Integer>
                candidatesEdgeEndNodesIndicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateEdgeEndNodeIndicies), true);


        SearchEdgeCandidates kernel = new SearchEdgeCandidates(context);
        int[] globalSizes = new int[]{candidateArray.length};

        // When
        CLEvent searchEdgeCandidatesEvent = kernel.count_edge_candidates(
                this.queue,
                2,
                3,
                this.d_adjacencies,
                this.d_adjacency_indicies,
                candidatesEdgeEndNodes,
                candidatesEdgeEndNodesIndicies,
                candidates_array,
                candidate_indicators,
                this.dataNodeCount,
                globalSizes,
                null
        );


        Pointer<Integer> outPtr = candidatesEdgeEndNodes.read(queue, searchEdgeCandidatesEvent); // blocks until add_floats finished

        // Then
        for (int i = 0; i < candidateArray.length; i++) {
            assertEquals(expectedEdgeEndNodes[i], (int) outPtr.get(i));
        }
    }

}