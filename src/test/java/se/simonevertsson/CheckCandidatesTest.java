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
                2,
                3,
                3,
                4,
                4
        };

        int[] dataAdjacencyIndicies = {
                0,
                2,
                4,
                5,
                5
        };


        int[] queryVertexLabels = {
                1,
                2,
                1,
                3
        };

        int n = 4;

        int[] candidateSet = {
                0,
                0,
                0,
                0
        };


        // Create OpenCL input and output buffers
        CLBuffer<Integer>
                labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabels), false),
                label_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabelIndicies), false),
                adjacency_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataAdjacencyIndicies), false),
                query_vertex_labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(Arrays.copyOfRange(queryVertexLabels, 0, 1)), false),
                c_set = context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(candidateSet, 0, n), false);


        CheckCandidates kernels = new CheckCandidates(context);
        int[] globalSizes = new int[] { n };

        // When
        CLEvent checkCandidatesEvent = kernels.check_candidates(
                queue,
                adjacency_indicies,
                2,
                labels,
                label_indicies,
                query_vertex_labels,
                1,
                c_set,
                n,
                globalSizes,
                null);

        Pointer<Integer> outPtr = c_set.read(queue, checkCandidatesEvent); // blocks until add_floats finished

        // Then

        assertEquals(1, (int) outPtr.get(0));
        assertEquals(0, (int) outPtr.get(1));
        assertEquals(0, (int) outPtr.get(2));
        assertEquals(0, (int) outPtr.get(3));
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
                2,
                3,
                3,
                4,
                4
        };

        int[] dataAdjacencyIndicies = {
                0,
                2,
                4,
                5,
                5
        };


        int[] queryVertexLabels = {
                1,
                2,
                1,
                3
        };

        int n = 4;

        int[] candidateSet = {
                0,
                0,
                0,
                0
        };



        // Create OpenCL input and output buffers
        CLBuffer<Integer>
                labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabels), false),
                label_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataLabelIndicies), false),
                adjacency_indicies = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(dataAdjacencyIndicies), false),
                query_vertex_labels = context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(Arrays.copyOfRange(queryVertexLabels, 1, 2)), false),
                c_set = context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(candidateSet, 0, n), false);


        CheckCandidates kernels = new CheckCandidates(context);
        int[] globalSizes = new int[] { n };

        // When
        CLEvent checkCandidatesEvent = kernels.check_candidates(
                queue,
                adjacency_indicies,
                2,
                labels,
                label_indicies,
                query_vertex_labels,
                1,
                c_set,
                n,
                globalSizes,
                null);

        Pointer<Integer> outPtr = c_set.read(queue, checkCandidatesEvent); // blocks until add_floats finished


        // Then

        assertEquals(0, (int) outPtr.get(0));
        assertEquals(1, (int) outPtr.get(1));
        assertEquals(0, (int) outPtr.get(2));
        assertEquals(0, (int) outPtr.get(3));
    }
}
