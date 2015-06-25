package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;

import java.io.IOException;

public class SolutionCombinationCounter {
    private final QueryKernels queryKernels;
    private final QueryContext queryContext;

    public SolutionCombinationCounter(QueryKernels queryKernels, QueryContext queryContext) {
        this.queryKernels = queryKernels;
        this.queryContext = queryContext;
    }

    public Pointer<Integer> countSolutionCombinations(CLBuffer<Integer> possibleSolutions, CandidateRelationships candidateRelationships, boolean startNodeVisited) throws IOException {
        int possibleSolutionCount = (int) possibleSolutions.getElementCount() / this.queryContext.queryNodeCount;

        CLBuffer<Integer>
                combinationCountsBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, possibleSolutionCount);

        int[] globalSizes = new int[]{(int) possibleSolutionCount};

        CLBuffer<Boolean> startNodeVisitedBuffer = this.queryKernels.context.createBuffer(
                CLMem.Usage.Input,
                Pointer.pointerToBooleans(startNodeVisited), true);

        CLEvent countSolutionCombinationsEvent = this.queryKernels.countSolutionCombinationsKernel.count_solution_combinations(
                this.queryKernels.queue,
                candidateRelationships.getQueryStartNodeId(),
                candidateRelationships.getQueryEndNodeId(),
                this.queryContext.queryNodeCount,
                possibleSolutions,
                candidateRelationships.getCandidateStartNodes(),
                candidateRelationships.getCandidateEndNodeIndices(),
                candidateRelationships.getCandidateEndNodes(),
                startNodeVisitedBuffer,
                candidateRelationships.getStartNodeCount(),
                combinationCountsBuffer,
                globalSizes,
                null
        );

        return combinationCountsBuffer.read(this.queryKernels.queue, countSolutionCombinationsEvent);
    }
}