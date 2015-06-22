package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;

public class DataBuffers {
    CLBuffer<Integer> dataAdjacencyIndicesBuffer;
    CLBuffer<Integer> dataLabelsBuffer;
    CLBuffer<Integer> dataLabelIndicesBuffer;
    CLBuffer<Integer> dataAdjacencesBuffer;

    public DataBuffers() {
    }
}