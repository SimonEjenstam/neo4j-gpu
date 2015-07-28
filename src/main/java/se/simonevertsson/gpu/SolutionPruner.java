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

    public PossibleSolutions prunePossibleSolutions(PossibleSolutions oldPossibleSolutions, CLBuffer<Boolean> validationIndicators, int[] outputIndexArray) throws IOException {
        int possibleSolutionCount = (int) oldPossibleSolutions.getSolutionElements().getElementCount() / this.queryContext.queryNodeCount;

        CLBuffer<Integer> outputIndices = this.queryKernels.context.createIntBuffer(
                CLMem.Usage.Input,
                IntBuffer.wrap(outputIndexArray),
                true);

        CLBuffer<Integer> prunedPossibleSolutionElements = this.queryKernels.context.createIntBuffer(
                CLMem.Usage.Input,
                outputIndexArray[outputIndexArray.length - 1] * this.queryContext.queryNodeCount
        );

        CLBuffer<Integer> prunedPossibleSolutionRelationships = this.queryKernels.context.createIntBuffer(
                CLMem.Usage.Input,
                outputIndexArray[outputIndexArray.length - 1] * this.queryContext.queryGraph.relationships.size()
        );


        int[] globalSizes = new int[]{possibleSolutionCount};

        CLEvent pruneSolutionsEvent = this.queryKernels.pruneSolutionsKernel.prune_solutions(
                this.queryKernels.queue,
                this.queryContext.queryNodeCount,
                oldPossibleSolutions.getSolutionElements(),
                validationIndicators,
                outputIndices,
                prunedPossibleSolutionElements,
                globalSizes,
                null
        );

        pruneSolutionsEvent.waitFor();

        PossibleSolutions prunedPossibleSolutions = new PossibleSolutions(prunedPossibleSolutionElements, prunedPossibleSolutionRelationships);

        return prunedPossibleSolutions;
    }
}