package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;

import java.io.IOException;

public class SolutionValidator {
    private final QueryKernels queryKernels;

    public SolutionValidator(QueryKernels queryKernels) {
        this.queryKernels = queryKernels;
    }

    CLBuffer<Boolean> validateSolutions(CLBuffer<Integer> possibleSolutions, int possibleSolutionCount, int startNodeId, int endNodeId, CandidateRelationships candidateRelationships) throws IOException {
        CLBuffer<Boolean> validationIndicators = this.queryKernels.context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToBooleans(new boolean[possibleSolutionCount]));

        int[] globalSizes = new int[]{possibleSolutionCount};

        CLEvent validateSolutionsEvent = this.queryKernels.validateSolutionsKernel.validate_solutions(
                this.queryKernels.queue,
                startNodeId,
                endNodeId,
                possibleSolutions,
                candidateRelationships.getCandidateStartNodes(),
                candidateRelationships.getCandidateEndNodeIndices(),
                candidateRelationships.getCandidateEndNodes(),
                candidateRelationships.getStartNodeCount(),
                validationIndicators,
                globalSizes,
                null
        );

        validateSolutionsEvent.waitFor();

        return validationIndicators;
    }
}