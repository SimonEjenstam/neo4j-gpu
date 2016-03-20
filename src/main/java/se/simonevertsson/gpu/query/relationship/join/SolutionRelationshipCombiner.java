package se.simonevertsson.gpu.query.relationship.join;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLEvent;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;
import se.simonevertsson.gpu.query.QueryContext;
import se.simonevertsson.gpu.kernel.QueryKernels;
import se.simonevertsson.gpu.query.relationship.search.CandidateRelationships;

import java.io.IOException;

/**
 * Created by simon.evertsson on 2015-08-03.
 */
public class SolutionRelationshipCombiner {
    private final QueryKernels queryKernels;
    private final QueryContext queryContext;

    public SolutionRelationshipCombiner(QueryKernels queryKernels, QueryContext queryContext) {
        this.queryKernels = queryKernels;
        this.queryContext = queryContext;
    }

    public CLBuffer<Integer> combineRelationships(PossibleSolutions possibleSolutions, CandidateRelationships candidateRelationships, int[] outputIndicesArray) throws IOException {

        CLBuffer<Integer> outputIndicesBuffer = this.queryKernels.context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToInts(outputIndicesArray),
                true);

        CLBuffer<Integer> relationshipIndicesBuffer = this.queryKernels.context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToInts(new int[outputIndicesArray[outputIndicesArray.length - 1]]),
                true);


        int[] globalSizes = new int[]{possibleSolutions.getSolutionCount()};

        CLEvent validateSolutionsEvent = this.queryKernels.combineRelationshipsKernel.combine_relationships(
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
                outputIndicesBuffer,
                relationshipIndicesBuffer,
                globalSizes,
                null
        );

        validateSolutionsEvent.waitFor();

        return relationshipIndicesBuffer;
    }
}
