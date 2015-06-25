package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;

import java.io.IOException;
import java.nio.IntBuffer;

public class SolutionCombinationGenerator {
    private final QueryKernels queryKernels;
    private final QueryContext queryContext;

    public SolutionCombinationGenerator(QueryKernels queryKernels, QueryContext queryContext) {
        this.queryKernels = queryKernels;
        this.queryContext = queryContext;
    }

    public CLBuffer<Integer> generateSolutionCombinations(CLBuffer<Integer> oldPossibleSolutions, CandidateRelationships candidateRelationships, boolean startNodeVisited, int[] combinationIndices) throws IOException {
        int oldPossibleSolutionCount = (int) oldPossibleSolutions.getElementCount() / this.queryContext.queryNodeCount;

        int totalCombinationCount = combinationIndices[combinationIndices.length - 1];

        int[] globalSizes = new int[]{oldPossibleSolutionCount};

        CLBuffer<Boolean> startNodeVisitedBuffer = this.queryKernels.context.createBuffer(
                CLMem.Usage.Input,
                Pointer.pointerToBooleans(startNodeVisited), true);

        CLBuffer<Integer>
                combinationIndicesBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(combinationIndices), true),
                possibleSolutions = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, totalCombinationCount * this.queryContext.queryNodeCount);

        CLEvent generateSolutionCombinationsEvent = this.queryKernels.generateSolutionCombinationsKernel.generate_solution_combinations(
                this.queryKernels.queue,
                candidateRelationships.getQueryStartNodeId(),
                candidateRelationships.getQueryEndNodeId(),
                this.queryContext.queryNodeCount,
                oldPossibleSolutions,
                combinationIndicesBuffer,
                candidateRelationships.getCandidateStartNodes(),
                candidateRelationships.getCandidateEndNodeIndices(),
                candidateRelationships.getCandidateEndNodes(),
                startNodeVisitedBuffer,
                candidateRelationships.getStartNodeCount(),
                possibleSolutions,
                globalSizes,
                null
        );
        generateSolutionCombinationsEvent.waitFor();

        return possibleSolutions;
    }
}