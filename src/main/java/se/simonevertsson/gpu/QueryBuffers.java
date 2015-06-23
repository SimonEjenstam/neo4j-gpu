package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import org.bridj.Pointer;

public class QueryBuffers {
    public CLBuffer<Boolean> candidateIndicatorsBuffer;
    CLBuffer<Integer> queryNodeLabelsBuffer;
    CLBuffer<Integer> queryNodeLabelIndicesBuffer;
    CLBuffer<Integer> queryNodeRelationshipsBuffer;
    CLBuffer<Integer> queryRelationshipIndicesBuffer;
    public Pointer<Boolean> candidateIndicatorsPointer;
    public CLBuffer<Integer> queryRelationshipTypesBuffer;

    public QueryBuffers() {
    }
}