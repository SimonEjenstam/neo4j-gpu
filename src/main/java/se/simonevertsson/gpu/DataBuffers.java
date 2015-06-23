package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;

public class DataBuffers {
    CLBuffer<Integer> dataRelationshipIndicesBuffer;
    CLBuffer<Integer> dataLabelsBuffer;
    CLBuffer<Integer> dataLabelIndicesBuffer;
    CLBuffer<Integer> dataNodeRelationshipsBuffer;
    public CLBuffer<Integer> dataRelationshipTypesBuffer;

    public DataBuffers() {
    }
}