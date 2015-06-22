package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import org.bridj.Pointer;

public class QueryBuffers {
    CLBuffer<Boolean> candidateIndicatorsBuffer;
    CLBuffer<Integer> queryNodeLabelsBuffer;
    CLBuffer<Integer> queryNodeLabelIndicesBuffer;
    CLBuffer<Integer> queryNodeAdjacenciesBuffer;
    CLBuffer<Integer> queryNodeAdjacencyIndicesBuffer;
    Pointer<Boolean> candidateIndicatorsPointer;

    public QueryBuffers() {
    }
}