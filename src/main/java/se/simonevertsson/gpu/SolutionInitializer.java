package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLMem;
import org.bridj.Pointer;

import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class SolutionInitializer {
    private final QueryContext queryContext;
    private final QueryKernels queryKernels;

    public SolutionInitializer(QueryContext queryContext, QueryKernels queryKernels) {
        this.queryContext = queryContext;
        this.queryKernels = queryKernels;
    }

    public CLBuffer<Integer> initializePossibleSolutions(HashMap<Integer, CandidateRelationships> candidateRelationshipsHashMap, ArrayList<Integer> visitedQueryRelationships, ArrayList<Integer> visitedQueryNodes) {
        int minCandidatesQueryRelationshipId = findQueryRelationshipIdWithMinimumCandidates(candidateRelationshipsHashMap);
        CandidateRelationships initialCandidateRelationships = candidateRelationshipsHashMap.get(minCandidatesQueryRelationshipId);

        int[] initialPossibleSolutionsArray = createInitialPossibleSolutions(initialCandidateRelationships);

        CLBuffer<Integer>
                possiblePartialSolutions = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(initialPossibleSolutionsArray), true);

        visitedQueryRelationships.add(minCandidatesQueryRelationshipId);
        visitedQueryNodes.add(initialCandidateRelationships.getQueryStartNodeId());
        visitedQueryNodes.add(initialCandidateRelationships.getQueryEndNodeId());

//        System.out.println("Initial possible solutions");
//        System.out.println(Arrays.toString(initialPossibleSolutionsArray));

        return possiblePartialSolutions;
    }

    public int[] createInitialPossibleSolutions(CandidateRelationships candidateRelationships) {
        int solutionNodeCount = this.queryContext.queryGraph.nodes.size();
        int partialSolutionCount = candidateRelationships.getEndNodeCount();
        int[] initialPossibleSolutionsArray = new int[solutionNodeCount * partialSolutionCount];

        /* Initially mark all nodes in all possible solutions as unknown (-1) */
        Arrays.fill(initialPossibleSolutionsArray, -1);

        Pointer<Integer> candidateStartNodes = candidateRelationships.getCandidateStartNodes().read(this.queryKernels.queue);
        Pointer<Integer> candidateEndNodes = candidateRelationships.getCandidateEndNodes().read(this.queryKernels.queue);
        Pointer<Integer> candidateEndNodeIndicies = candidateRelationships.getCandidateEndNodeIndices().read(this.queryKernels.queue);
        int addedPossibleSolutions = 0;
        int indexOffset = 0;
        while (addedPossibleSolutions < candidateRelationships.getEndNodeCount()) {

            for (int i = 0; i < candidateRelationships.getCandidateStartNodes().getElementCount(); i++) {

                int startNode = candidateStartNodes.get(i);
                int maxIndex = candidateEndNodeIndicies.get(i + 1);

                for (int j = candidateEndNodeIndicies.get(i); j < maxIndex; j++) {
                    int endNode = candidateEndNodes.get(j);
                    int solutionStartNodeIndex = indexOffset + candidateRelationships.getQueryStartNodeId();
                    int solutionEndNodeIndex = indexOffset + candidateRelationships.getQueryEndNodeId();
                    initialPossibleSolutionsArray[solutionStartNodeIndex] = startNode;
                    initialPossibleSolutionsArray[solutionEndNodeIndex] = endNode;

                    addedPossibleSolutions++;
                    indexOffset = addedPossibleSolutions * solutionNodeCount;
                }
            }
        }
        return initialPossibleSolutionsArray;
    }

    public int findQueryRelationshipIdWithMinimumCandidates(HashMap<Integer, CandidateRelationships> edgeCandidatesHashMap) {
        int minimumId = -1;
        int minimumCandidateCount = Integer.MAX_VALUE;
        for (int relationshipId : edgeCandidatesHashMap.keySet()) {
            CandidateRelationships candidateRelationships = edgeCandidatesHashMap.get(relationshipId);
            int candidateCount = candidateRelationships.getEndNodeCount();
            if (candidateCount <= minimumCandidateCount) {
                minimumCandidateCount = candidateCount;
                minimumId = relationshipId;
            }
        }
        return minimumId;
    }
}