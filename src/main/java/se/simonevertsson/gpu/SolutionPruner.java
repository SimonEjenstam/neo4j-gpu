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

    public PossibleSolutions prunePossibleSolutions(CandidateRelationships candidateRelationships, PossibleSolutions oldPossibleSolutions, CLBuffer<Integer> validRelationships, int[] outputIndexArray) throws IOException {
        int possibleSolutionCount = (int) oldPossibleSolutions.getSolutionElements().getElementCount() / this.queryContext.queryNodeCount;

        CLBuffer<Integer> outputIndices = this.queryKernels.context.createIntBuffer(
                CLMem.Usage.Input,
                IntBuffer.wrap(outputIndexArray),
                true);

        CLBuffer<Integer> prunedPossibleSolutionElements = this.queryKernels.context.createIntBuffer(
                CLMem.Usage.Output,
                outputIndexArray[outputIndexArray.length - 1] * this.queryContext.queryNodeCount
        );

        CLBuffer<Integer> prunedPossibleSolutionRelationships = this.queryKernels.context.createIntBuffer(
                CLMem.Usage.Output,
                outputIndexArray[outputIndexArray.length - 1] * this.queryContext.queryRelationshipCount
        );

        int relationshipId = this.queryContext.gpuQuery.getRelationshipIdDictionary()
                .getQueryId(candidateRelationships.getRelationship().getId());

        int[] globalSizes = new int[]{possibleSolutionCount};

        CLEvent pruneSolutionsEvent = this.queryKernels.pruneSolutionsKernel.prune_solutions(
                this.queryKernels.queue,
                this.queryContext.queryNodeCount,
                this.queryContext.queryRelationshipCount,
                relationshipId,

                oldPossibleSolutions.getSolutionElements(),
                oldPossibleSolutions.getSolutionRelationships(),
                validRelationships,
                outputIndices,
                prunedPossibleSolutionElements,
                prunedPossibleSolutionRelationships,
                globalSizes,
                null
        );

        pruneSolutionsEvent.waitFor();

        PossibleSolutions prunedPossibleSolutions = new PossibleSolutions(prunedPossibleSolutionElements, prunedPossibleSolutionRelationships, this.queryContext, this.queryKernels.queue);

        return prunedPossibleSolutions;
    }
}