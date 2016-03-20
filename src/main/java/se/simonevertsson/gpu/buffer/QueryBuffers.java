package se.simonevertsson.gpu.buffer;

import com.nativelibs4java.opencl.CLBuffer;
import org.bridj.Pointer;

public class QueryBuffers {
    public CLBuffer<Boolean> candidateIndicatorsBuffer;
    public CLBuffer<Integer> queryNodeLabelsBuffer;
    public CLBuffer<Integer> queryNodeLabelIndicesBuffer;
    public CLBuffer<Integer> queryNodeRelationshipsBuffer;
    public CLBuffer<Integer> queryRelationshipIndicesBuffer;
    public Pointer<Boolean> candidateIndicatorsPointer;
    public CLBuffer<Integer> queryRelationshipTypesBuffer;
}