package se.simonevertsson;

import com.nativelibs4java.opencl.*;
import junit.framework.TestCase;
import org.bridj.Pointer;

import java.nio.IntBuffer;
import java.util.Arrays;

/**
 * Created by simon.evertsson on 2015-05-18.
 */
public class CheckCandidatesTest extends TestCase {



    public void testReturnsTrueForCorrectCandidate() throws Exception {
        // Given
        CLContext context = JavaCL.createBestContext();
        CLQueue queue = context.createDefaultQueue();

        int orderCounter = 0;

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


        int[] queryVertexLabels = {
                2,
                1,
                1,
                3
        };

        int n = 4;

        int dataNodeCount = 4;

        int queryNodeCount = 4;


        boolean[] candidateSet = new boolean[dataNodeCount*queryNodeCount];


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
                query_vertex_labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryVertexLabels), true);
        CLBuffer<Boolean>
                c_set = context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(candidateSet));


        CheckCandidates kernels = new CheckCandidates(context);
        int[] globalSizes = new int[] { n };

        // When
        CLEvent checkCandidatesEvent = kernels.check_candidates(
                queue,
                query_vertex_labels,
                1,
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

        Pointer<Boolean> outPtr = c_set.read(queue, checkCandidatesEvent); // blocks until add_floats finished

        // Then
        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateSet[i], (boolean) outPtr.get(i));
        }
    }


    public void testReturnsTrueForCorrectCandidate2() throws Exception {
        // Given
        CLContext context = JavaCL.createBestContext();
        CLQueue queue = context.createDefaultQueue();

        int orderCounter = 0;

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


        int[] queryVertexLabels = {
                2,
                1,
                1,
                3
        };

        int dataNodeCount = 4;

        int queryNodeCount = 4;

        boolean[] candidateSet = new boolean[dataNodeCount*queryNodeCount];


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
                query_vertex_labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(queryVertexLabels), true);
        CLBuffer<Boolean>
                 c_set = context.createBuffer(CLMem.Usage.Output, Pointer.pointerToBooleans(candidateSet));


        CheckCandidates kernels = new CheckCandidates(context);
        int[] globalSizes = new int[] { dataNodeCount };

        // When
        CLEvent checkCandidatesEvent = kernels.check_candidates(
                queue,
                query_vertex_labels,
                0,
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

        Pointer<Boolean> outPtr = c_set.read(queue, checkCandidatesEvent); // blocks until finished

        // Then
        for(int i = 0; i < dataNodeCount * queryNodeCount; i++) {
            assertEquals(expectedCandidateSet[i], (boolean) outPtr.get(i));
        }
    }
}
