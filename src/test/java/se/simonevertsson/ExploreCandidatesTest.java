package se.simonevertsson;

import com.nativelibs4java.opencl.*;
import junit.framework.TestCase;
import org.bridj.Pointer;

import java.nio.IntBuffer;
import java.nio.LongBuffer;

/**
 * Created by simon.evertsson on 2015-05-18.
 */
public class ExploreCandidatesTest extends TestCase {

    long[] queryAdjacencies = {
            2,
            3,
            1,
            2,
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
            2,
            1,
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

    public void testReturnsTrueForCorrectCandidates() throws Exception {
        // Given
        int[] candidateArray = new int[] {
                0,2
        };

        boolean[] candidateIndicators = {
                true, false, false, false,
                false, true, false, false,
                true, false, true, false,
                false, false, false, true,
        };


        boolean[] expectedCandidateIndicators = {
                true, false, false, false,
                false, true, false, false,
                false, false, true, false,
                false, false, false, true,
        };

        CLBuffer<Boolean>
                candidate_indicators = context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(candidateIndicators));
        CLBuffer<Integer>
                candidates_array = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateArray), true);


        ExploreCandidates kernel = new ExploreCandidates(context);
        int[] globalSizes = new int[] { candidateArray.length };

        // When
        CLEvent checkCandidatesEvent = kernel.explore_candidates(
                queue,
                2,
                q_adjacencies,
                q_adjacency_indicies,
                q_labels,
                q_label_indicies,
                0,
                1,
                d_adjacencies,
                d_adjacency_indicies,
                d_labels,
                d_label_indicies,
                candidates_array,
                candidate_indicators,
                dataNodeCount,
                globalSizes,
                null
        );

        Pointer<Boolean> outPtr = candidate_indicators.read(queue, checkCandidatesEvent); // blocks until add_floats finished

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

        // Then
        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateIndicators[i], (boolean) outPtr.get(i));
        }
    }
}
