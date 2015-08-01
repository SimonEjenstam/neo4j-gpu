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

    public PossibleSolutions generateSolutionCombinations(PossibleSolutions oldPossibleSolutions, CandidateRelationships candidateRelationships, boolean startNodeVisited, int[] combinationIndices) throws IOException {
        int oldPossibleSolutionCount = (int) oldPossibleSolutions.getSolutionElements().getElementCount() / this.queryContext.queryNodeCount;

        int totalCombinationCount = combinationIndices[combinationIndices.length - 1];

        int[] globalSizes = new int[]{oldPossibleSolutionCount};

        CLBuffer<Boolean> startNodeVisitedBuffer = this.queryKernels.context.createBuffer(
                CLMem.Usage.Input,
                Pointer.pointerToBooleans(startNodeVisited), true);

        CLBuffer<Integer>
                combinationIndicesBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(combinationIndices), true),
                newPossibleSolutionElements = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, totalCombinationCount * this.queryContext.queryNodeCount),
                newPossibleSolutionRelationships = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, totalCombinationCount * this.queryContext.queryRelationshipCount);

        int relationshipId = this.queryContext.gpuQuery.getRelationshipIdDictionary()
                .getQueryId(candidateRelationships.getRelationship().getId());

        CLEvent generateSolutionCombinationsEvent = this.queryKernels.generateSolutionCombinationsKernel.generate_solution_combinations(
                this.queryKernels.queue,
                candidateRelationships.getQueryStartNodeId(),
                candidateRelationships.getQueryEndNodeId(),
                relationshipId,
                this.queryContext.queryNodeCount,
                this.queryContext.queryRelationshipCount,
                oldPossibleSolutions.getSolutionElements(),
                oldPossibleSolutions.getSolutionRelationships(),
                combinationIndicesBuffer,
                candidateRelationships.getCandidateStartNodes(),
                candidateRelationships.getCandidateEndNodeIndices(),
                candidateRelationships.getCandidateEndNodes(),
                candidateRelationships.getRelationshipIndices(),
                startNodeVisitedBuffer,
                candidateRelationships.getStartNodeCount(),
                newPossibleSolutionElements,
                newPossibleSolutionRelationships,
                globalSizes,
                null
        );
        generateSolutionCombinationsEvent.waitFor();

        PossibleSolutions newPossibleSolutions = new PossibleSolutions(newPossibleSolutionElements, newPossibleSolutionRelationships);

        return newPossibleSolutions;
    }
}