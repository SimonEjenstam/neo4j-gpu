package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;

import javax.management.Query;
import java.io.IOException;
import java.nio.IntBuffer;

public class SolutionPruner {
    private final QueryKernels queryKernels;
    private final QueryContext queryContext;

    public SolutionPruner(QueryKernels queryKernels, QueryContext queryContext) {
        this.queryKernels = queryKernels;
        this.queryContext = queryContext;
    }

    public CLBuffer<Integer> prunePossibleSolutions(CLBuffer<Integer> oldPossibleSolutions, CLBuffer<Boolean> validationIndicators, int[] outputIndexArray) throws IOException {
        int possibleSolutionCount = (int) oldPossibleSolutions.getElementCount() / this.queryContext.queryNodeCount;

        CLBuffer<Integer> outputIndices = this.queryKernels.context.createIntBuffer(
                CLMem.Usage.Input,
                IntBuffer.wrap(outputIndexArray),
                true);

        CLBuffer<Integer> prunedPossibleSolutions = this.queryKernels.context.createIntBuffer(
                CLMem.Usage.Input,
                outputIndexArray[outputIndexArray.length - 1] * this.queryContext.queryNodeCount
        );

        int[] globalSizes = new int[]{possibleSolutionCount};

        CLEvent pruneSolutionsEvent = this.queryKernels.pruneSolutionsKernel.prune_solutions(
                this.queryKernels.queue,
                this.queryContext.queryNodeCount,
                oldPossibleSolutions,
                validationIndicators,
                outputIndices,
                prunedPossibleSolutions,
                globalSizes,
                null
        );

        pruneSolutionsEvent.waitFor();

        return prunedPossibleSolutions;
    }
}