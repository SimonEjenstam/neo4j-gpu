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

    public PossibleSolutions initializePossibleSolutions(HashMap<Integer, CandidateRelationships> candidateRelationshipsHashMap, ArrayList<Integer> visitedQueryRelationships, ArrayList<Integer> visitedQueryNodes) {
        int minCandidatesQueryRelationshipId = findQueryRelationshipIdWithMinimumCandidates(candidateRelationshipsHashMap);
        CandidateRelationships initialCandidateRelationships = candidateRelationshipsHashMap.get(minCandidatesQueryRelationshipId);

        PossibleSolutions initialPossibleSolutions = createInitialPossibleSolutions(initialCandidateRelationships);

        visitedQueryRelationships.add(minCandidatesQueryRelationshipId);
        visitedQueryNodes.add(initialCandidateRelationships.getQueryStartNodeId());
        visitedQueryNodes.add(initialCandidateRelationships.getQueryEndNodeId());

        return initialPossibleSolutions;
    }

    public PossibleSolutions createInitialPossibleSolutions(CandidateRelationships candidateRelationships) {
            int solutionNodeCount = this.queryContext.queryGraph.nodes.size();
            int solutionRelationshipCount = this.queryContext.queryGraph.relationships.size();

            int initialSolutionsCount = candidateRelationships.getEndNodeCount();
            int[] initialSolutionElementsArray = new int[solutionNodeCount * initialSolutionsCount];
            int[] initialSolutionRelationshipsArray = new int[solutionRelationshipCount * initialSolutionsCount];

            /* Initially mark all nodes in all possible solutions as unknown (-1) */
            Arrays.fill(initialSolutionElementsArray, -1);
            Arrays.fill(initialSolutionRelationshipsArray, -1);

            Pointer<Integer> candidateStartNodes = candidateRelationships.getCandidateStartNodes().read(this.queryKernels.queue);
            Pointer<Integer> candidateEndNodes = candidateRelationships.getCandidateEndNodes().read(this.queryKernels.queue);
            Pointer<Integer> candidateEndNodeIndicies = candidateRelationships.getCandidateEndNodeIndices().read(this.queryKernels.queue);
            Pointer<Integer> candidateRelationshipIndices = candidateRelationships.getRelationshipIndices().read(this.queryKernels.queue);
            int addedPossibleSolutions = 0;
            int solutionElementIndexOffset = 0;
            int solutionRelationshipIndexOffset = 0;

            int relationshipId = this.queryContext.gpuQuery.getRelationshipIdDictionary()
                    .getQueryId(candidateRelationships.getRelationship().getId());
            int startNodeId = candidateRelationships.getQueryStartNodeId();
            int endNodeId = candidateRelationships.getQueryEndNodeId();

            while (addedPossibleSolutions < candidateRelationships.getEndNodeCount()) {

                for (int i = 0; i < candidateRelationships.getCandidateStartNodes().getElementCount(); i++) {

                    int startNode = candidateStartNodes.get(i);
                    int maxIndex = candidateEndNodeIndicies.get(i + 1);

                    for (int j = candidateEndNodeIndicies.get(i); j < maxIndex; j++) {
                        int endNode = candidateEndNodes.get(j);
                        int relationshipIndex  = candidateRelationshipIndices.get(j);
                        int solutionElementStartNodeIndex = solutionElementIndexOffset + startNodeId;
                        int solutionElementEndNodeIndex = solutionElementIndexOffset + endNodeId;
                        int solutionRelationshipIndex = solutionRelationshipIndexOffset + relationshipId;

                        initialSolutionElementsArray[solutionElementStartNodeIndex] = startNode;
                        initialSolutionElementsArray[solutionElementEndNodeIndex] = endNode;
                        initialSolutionRelationshipsArray[solutionRelationshipIndex] = relationshipIndex;

                        addedPossibleSolutions++;
                        solutionElementIndexOffset = addedPossibleSolutions * solutionNodeCount;
                        solutionRelationshipIndexOffset = addedPossibleSolutions * solutionRelationshipCount;
                    }
                }
            }

            CLBuffer<Integer>
                    initialSolutionElements = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(initialSolutionElementsArray), true);

            CLBuffer<Integer>
                    initialSolutionRelationships = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(initialSolutionRelationshipsArray), true);

            PossibleSolutions initialPossibleSolution = new PossibleSolutions(initialSolutionElements, initialSolutionRelationships, this.queryContext, this.queryKernels.queue);

            return initialPossibleSolution;
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