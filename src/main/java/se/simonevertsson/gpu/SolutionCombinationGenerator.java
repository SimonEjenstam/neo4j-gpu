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

    CLBuffer<Integer> generateSolutionCombinations(CLBuffer<Integer> oldPossibleSolutions, int oldPossibleSolutionCount, CandidateRelationships candidateRelationships, int startNodeId, int endNodeId, boolean startNodeVisited, int[] combinationIndicies) throws IOException {
        int totalCombinationCount = combinationIndicies[combinationIndicies.length - 1];

        int[] globalSizes = new int[]{oldPossibleSolutionCount};

        CLBuffer<Boolean> startNodeVisitedBuffer = this.queryKernels.context.createBuffer(
                CLMem.Usage.Input,
                Pointer.pointerToBooleans(new boolean[]{startNodeVisited}), true);

        CLBuffer<Integer>
                combinationIndicesBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(combinationIndicies), true),
                possibleSolutions = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, totalCombinationCount * this.queryContext.queryNodeCount);

        CLEvent generateSolutionCombinationsEvent = this.queryKernels.generateSolutionCombinationsKernel.generate_solution_combinations(
                this.queryKernels.queue,
                startNodeId,
                endNodeId,
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