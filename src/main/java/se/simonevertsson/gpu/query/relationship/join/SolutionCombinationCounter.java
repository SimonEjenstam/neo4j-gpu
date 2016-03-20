package se.simonevertsson.gpu.query.relationship.join;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;
import se.simonevertsson.gpu.query.QueryContext;
import se.simonevertsson.gpu.kernel.QueryKernels;
import se.simonevertsson.gpu.query.relationship.search.CandidateRelationships;

import java.io.IOException;

public class SolutionCombinationCounter {
  private final QueryKernels queryKernels;
  private final QueryContext queryContext;

  public SolutionCombinationCounter(QueryKernels queryKernels, QueryContext queryContext) {
    this.queryKernels = queryKernels;
    this.queryContext = queryContext;
  }

  public Pointer<Integer> countSolutionCombinations(PossibleSolutions possibleSolutions, CandidateRelationships candidateRelationships, boolean startNodeVisited) throws IOException {
    int possibleSolutionCount = (int) possibleSolutions.getSolutionElements().getElementCount() / this.queryContext.queryNodeCount;

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
        this.queryContext.queryRelationshipCount,
        possibleSolutions.getSolutionElements(),
        possibleSolutions.getSolutionRelationships(),
        candidateRelationships.getCandidateStartNodes(),
        candidateRelationships.getCandidateEndNodeIndices(),
        candidateRelationships.getCandidateEndNodes(),
        candidateRelationships.getRelationshipIndices(),
        startNodeVisitedBuffer,
        candidateRelationships.getStartNodeCount(),
        combinationCountsBuffer,
        globalSizes,
        null
    );

    return combinationCountsBuffer.read(this.queryKernels.queue, countSolutionCombinationsEvent);
  }
}