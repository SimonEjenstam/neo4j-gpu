package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;

import java.io.IOException;

public class SolutionCombinationCounter {
    private final QueryKernels queryKernels;

    public SolutionCombinationCounter(QueryKernels queryKernels) {
        this.queryKernels = queryKernels;
    }

    Pointer<Integer> countSolutionCombinations(CLBuffer<Integer> possiblePartialSolutions, CandidateRelationships candidateRelationships, int startNodeId, int endNodeId, boolean startNodeVisited, int combinationCountsLength, CLBuffer<Integer> combinationCounts) throws IOException {
        int[] globalSizes = new int[]{combinationCountsLength};

        CLBuffer<Boolean> startNodeVisitedBuffer = this.queryKernels.context.createBuffer(
                CLMem.Usage.Input,
                Pointer.pointerToBooleans(new boolean[] {startNodeVisited}), true);

        CLEvent countSolutionCombinationsEvent = this.queryKernels.countSolutionCombinationsKernel.count_solution_combinations(
                this.queryKernels.queue,
                startNodeId,
                endNodeId,
                possiblePartialSolutions,
                candidateRelationships.getCandidateStartNodes(),
                candidateRelationships.getCandidateEndNodeIndices(),
                candidateRelationships.getCandidateEndNodes(),
                startNodeVisitedBuffer,
                candidateRelationships.getStartNodeCount(),
                combinationCounts,
                globalSizes,
                null
        );

        return combinationCounts.read(this.queryKernels.queue, countSolutionCombinationsEvent);
    }
}