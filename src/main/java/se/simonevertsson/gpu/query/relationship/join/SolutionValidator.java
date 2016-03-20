package se.simonevertsson.gpu.query.relationship.join;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;
import se.simonevertsson.gpu.query.QueryContext;
import se.simonevertsson.gpu.kernel.QueryKernels;
import se.simonevertsson.gpu.query.relationship.search.CandidateRelationships;

import java.io.IOException;

public class SolutionValidator {
    private final QueryKernels queryKernels;
    private final QueryContext queryContext;

    public SolutionValidator(QueryKernels queryKernels, QueryContext queryContext) {
        this.queryKernels = queryKernels;
        this.queryContext = queryContext;
    }

    public CLBuffer<Boolean> validateSolutions(PossibleSolutions possibleSolutions, CandidateRelationships candidateRelationships) throws IOException {
        int possibleSolutionCount = (int) possibleSolutions.getSolutionElements().getElementCount() / this.queryContext.queryNodeCount;

        CLBuffer<Boolean> validationIndicators = this.queryKernels.context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToBooleans(new boolean[possibleSolutionCount]));

        int[] globalSizes = new int[]{possibleSolutionCount};

        CLEvent validateSolutionsEvent = this.queryKernels.validateSolutionsKernel.validate_solutions(
                this.queryKernels.queue,
                candidateRelationships.getQueryStartNodeId(),
                candidateRelationships.getQueryEndNodeId(),
                this.queryContext.queryNodeCount,
                this.queryContext.queryRelationshipCount,
                possibleSolutions.getSolutionElements(),
                possibleSolutions.getSolutionRelationships(),
                candidateRelationships.getCandidateStartNodes(),
                candidateRelationships.getCandidateEndNodeIndices(),
                candidateRelationships.getCandidateEndNodes(),
                candidateRelationships.getRelationshipIndices(),
                candidateRelationships.getStartNodeCount(),
                validationIndicators,
                globalSizes,
                null
        );

        validateSolutionsEvent.waitFor();

        return validationIndicators;
    }
}