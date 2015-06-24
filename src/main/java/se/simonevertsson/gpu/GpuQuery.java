package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.*;
import org.bridj.Pointer;
import org.neo4j.graphdb.Node;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class GpuQuery {
    private QueryContext queryContext;
    private final QueryKernels queryKernels;
    private BufferContainer bufferContainer;

    public GpuQuery(QueryContext queryContext) throws IOException {
        this.queryContext = queryContext;
        this.queryKernels = new QueryKernels();
        this.bufferContainer = BufferContainerGenerator.generateBufferContainer(this.queryContext, this.queryKernels);
    }

    public void executeQuery(ArrayList<Node> visitOrder) throws IOException {

        /****** Candidate initialization step ******/
        CandidateInitializer candidateInitializer =
                new CandidateInitializer(this.queryContext, this.queryKernels, this.bufferContainer);
        candidateInitializer.candidateInitialization(visitOrder);


        /****** Candidate refinement step ******/
        CandidateRefinement candidateRefinement =
                new CandidateRefinement(this.queryContext, this.queryKernels, this.bufferContainer);
        candidateRefinement.refine(visitOrder);

        /****** Candidate edge searching step ******/
        CandidateRelationshipSearcher candidateRelationshipSearcher =
                new CandidateRelationshipSearcher(this.queryContext, this.queryKernels, this.bufferContainer);
        HashMap<Integer, RelationshipCandidates> relationshipCandidatesHashMap = candidateRelationshipSearcher.searchCandidateRelationships();

//        System.out.println("--------------JOINING CANDIDATE EDGES-------------");
        candidateEdgeJoin(relationshipCandidatesHashMap);
    }

    private void candidateEdgeJoin(HashMap<Integer, RelationshipCandidates> edgeCandidatesHashMap) throws IOException {
        ArrayList<Integer> visitedQueryEdges = new ArrayList<Integer>();
        ArrayList<Integer> visitedQueryVertices = new ArrayList<Integer>();

        CLBuffer<Integer> possibleSolutions = initializePossiblePartialSolutions(edgeCandidatesHashMap, visitedQueryEdges, visitedQueryVertices);

        for(int relationshipId : edgeCandidatesHashMap.keySet()) {
            if(!visitedQueryEdges.contains(relationshipId)) {
                RelationshipCandidates relationshipCandidates = edgeCandidatesHashMap.get(relationshipId);
                int startNodeId = relationshipCandidates.getQueryStartNodeId();
                int endNodeId = relationshipCandidates.getQueryEndNodeId();
                boolean startNodeVisisted = visitedQueryVertices.contains(startNodeId);
                boolean endNodeVisisted = visitedQueryVertices.contains(endNodeId);

                int possibleSolutionCount = (int)possibleSolutions.getElementCount()/ this.queryContext.queryNodeCount;

                CLBuffer<Integer>
                        combinationCounts = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, possibleSolutionCount);

                if(startNodeVisisted && endNodeVisisted) {
                    /* Prune existing possible solutions */
                    CLBuffer<Boolean> validationIndicators = validateSolutions(possibleSolutions, possibleSolutionCount, startNodeId, endNodeId, relationshipCandidates);

                    Pointer<Boolean> validationIndicatorsPointer = validationIndicators.read(this.queryKernels.queue);
//                    System.out.println("Result after validation:");
//                    System.out.println(Arrays.toString(pointerToArray(validationIndicatorsPointer, possibleSolutionCount)));

                    int[] outputIndexArray = new int[possibleSolutionCount+1];
                    int validSolutionCount = 0;

                    if(validationIndicatorsPointer.get(0)) {
                        validSolutionCount++;
                    }

                    for (int i = 1; i < possibleSolutionCount; i++) {
                        int nextElement = validationIndicatorsPointer.get(i-1) ? 1 : 0;
                        outputIndexArray[i] = outputIndexArray[i-1] + nextElement;
                        if(validationIndicatorsPointer.get(i)) {
                            validSolutionCount++;
                        }
                    }

                    outputIndexArray[outputIndexArray.length-1] = validSolutionCount;

                    CLBuffer<Integer> prunedPossibleSolutions = prunePossibleSolutions(
                            possibleSolutions,
                            possibleSolutionCount,
                            validationIndicators,
                            outputIndexArray);

                    Pointer<Integer> prunedPossibleSolutionsPointer = prunedPossibleSolutions.read(this.queryKernels.queue);

                    int result[] = new int[validSolutionCount* this.queryContext.queryNodeCount];
                    int i = 0;
                    for(int element : prunedPossibleSolutionsPointer) {
                        result[i] = element;
                        i++;
                    }

//                    System.out.println("Solutions after pruning with candidates of (" + startNodeId + ", " + endNodeId + ")");
//                    System.out.println(Arrays.toString(result));

                    possibleSolutions = prunedPossibleSolutions;

                    visitedQueryEdges.add(relationshipId);

                } else if(startNodeVisisted || endNodeVisisted) {
                    /* Combine candidate edges with existing possible solutions */
                    Pointer<Integer> combinationCountsPointer = countSolutionCombinations(
                            possibleSolutions,
                            relationshipCandidates, startNodeId,
                            endNodeId, startNodeVisisted,
                            possibleSolutionCount,
                            combinationCounts);

                    int[] combinationIndicies = QueryUtils.generatePrefixScanArray(combinationCountsPointer, possibleSolutionCount);

                    CLBuffer<Integer> newPossibleSolutions = generateSolutionCombinations(
                            possibleSolutions,
                            possibleSolutionCount,
                            relationshipCandidates,
                            startNodeId,
                            endNodeId,
                            startNodeVisisted,
                            combinationIndicies);

                    Pointer<Integer> newPossibleSolutionsPointer = newPossibleSolutions.read(this.queryKernels.queue);

                    int result[] = new int[combinationIndicies[combinationIndicies.length-1]* this.queryContext.queryNodeCount];
                    int i = 0;
                    for(int element : newPossibleSolutionsPointer) {
                        result[i] = element;
                        i++;
                    }

//                    System.out.println("Combinations after combining with candidates of (" + startNodeId + ", " + endNodeId + ")");
//                    System.out.println(Arrays.toString(result));

                    possibleSolutions = newPossibleSolutions;

                    visitedQueryEdges.add(relationshipId);
                    if(startNodeVisisted) {
                        visitedQueryVertices.add(endNodeId);
                    } else {
                        visitedQueryVertices.add(startNodeId);
                    }
                }
            }
        }

        printFinalSolutions(possibleSolutions);

    }

    private void printFinalSolutions(CLBuffer<Integer> solutionsBuffer) {
        Pointer<Integer> solutionsPointer = solutionsBuffer.read(this.queryKernels.queue);
        int solutionCount = (int) (solutionsBuffer.getElementCount()/ this.queryContext.queryNodeCount);

        StringBuilder builder = new StringBuilder();
        builder.append("Final solutions:\n");

        for(int i = 0; i < solutionCount* queryContext.queryNodeCount; i++) {
            if(i % this.queryContext.queryNodeCount == 0) {
                builder.append("(");
                builder.append(solutionsPointer.get(i));
            } else {
                builder.append(", ");
                builder.append(solutionsPointer.get(i));
                if (i % this.queryContext.queryNodeCount == this.queryContext.queryNodeCount - 1) {
                    builder.append(")\n");
                }
            }
        }

        System.out.println(builder.toString());
    }

    private CLBuffer<Integer> prunePossibleSolutions(CLBuffer<Integer> oldPossibleSolutions, int possibleSolutionCount, CLBuffer<Boolean> validationIndicators, int[] outputIndexArray) throws IOException {
        CLBuffer<Integer> outputIndices = this.queryKernels.context.createIntBuffer(
                CLMem.Usage.Input,
                IntBuffer.wrap(outputIndexArray),
                true);

        CLBuffer<Integer> prunedPossibleSolutions = this.queryKernels.context.createIntBuffer(
                CLMem.Usage.Input,
                outputIndexArray[outputIndexArray.length - 1] * this.queryContext.queryNodeCount
        );

        int[] globalSizes = new int[] { possibleSolutionCount };

        CLEvent pruneSolutionsEvent = this.queryKernels.pruneSolutionsKernel.prune_solutions(
                this.queryKernels.queue,
                this.queryContext.queryNodeCount,
                oldPossibleSolutions,
                validationIndicators,
                outputIndices,
                prunedPossibleSolutions,
                globalSizes,
                null
        );

        pruneSolutionsEvent.waitFor();

        return prunedPossibleSolutions;
    }

    private CLBuffer<Boolean> validateSolutions(CLBuffer<Integer> possibleSolutions, int possibleSolutionCount, int startNodeId, int endNodeId, RelationshipCandidates relationshipCandidates) throws IOException {
        CLBuffer<Boolean> validationIndicators = this.queryKernels.context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToBooleans(new boolean[possibleSolutionCount]));

        int[] globalSizes = new int[] { possibleSolutionCount };

        CLEvent validateSolutionsEvent = this.queryKernels.validateSolutionsKernel.validate_solutions(
                this.queryKernels.queue,
                startNodeId,
                endNodeId,
                possibleSolutions,
                relationshipCandidates.getCandidateStartNodes(),
                relationshipCandidates.getCandidateEndNodeIndices(),
                relationshipCandidates.getCandidateEndNodes(),
                relationshipCandidates.getStartNodeCount(),
                validationIndicators,
                globalSizes,
                null
        );

        validateSolutionsEvent.waitFor();

        return validationIndicators;
    }

    private CLBuffer<Integer> generateSolutionCombinations(CLBuffer<Integer> oldPossibleSolutions, int oldPossibleSolutionCount, RelationshipCandidates relationshipCandidates, int startNodeId, int endNodeId, boolean startNodeVisisted, int[] combinationIndicies) throws IOException {
        int totalCombinationCount = combinationIndicies[combinationIndicies.length-1];

        int[] globalSizes = new int[] { oldPossibleSolutionCount };
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
                relationshipCandidates.getCandidateStartNodes(),
                relationshipCandidates.getCandidateEndNodeIndices(),
                relationshipCandidates.getCandidateEndNodes(),
                startNodeVisisted,
                relationshipCandidates.getStartNodeCount(),
                possibleSolutions,
                globalSizes,
                null
        );
        generateSolutionCombinationsEvent.waitFor();

        return possibleSolutions;
    }

    private Pointer<Integer> countSolutionCombinations(CLBuffer<Integer> possiblePartialSolutions, RelationshipCandidates relationshipCandidates, int startNodeId, int endNodeId, boolean startNodeVisisted, int combinationCountsLength, CLBuffer<Integer> combinationCounts) throws IOException {
        int[] globalSizes = new int[] { combinationCountsLength };
        CLEvent countSolutionCombinationsEvent = this.queryKernels.countSolutionCombinationsKernel.count_solution_combinations(
                this.queryKernels.queue,
                startNodeId,
                endNodeId,
                possiblePartialSolutions,
                relationshipCandidates.getCandidateStartNodes(),
                relationshipCandidates.getCandidateEndNodeIndices(),
                relationshipCandidates.getCandidateEndNodes(),
                startNodeVisisted,
                relationshipCandidates.getStartNodeCount(),
                combinationCounts,
                globalSizes,
                null
        );

        return combinationCounts.read(this.queryKernels.queue, countSolutionCombinationsEvent);
    }

    private CLBuffer<Integer> initializePossiblePartialSolutions(HashMap<Integer, RelationshipCandidates> edgeCandidatesHashMap, ArrayList<Integer> visitedQueryEdges, ArrayList<Integer> visitedQueryVertices) {
        int minCandidatesQueryEdgeId = findUnvisitedQueryEdgeId(edgeCandidatesHashMap, visitedQueryEdges);
        RelationshipCandidates initialRelationshipCandidates = edgeCandidatesHashMap.get(minCandidatesQueryEdgeId);

        int solutionSize = this.queryContext.queryGraph.nodes.size();
        int initalPartialSolutionCount = initialRelationshipCandidates.getTotalCount();
        int[] possiblePartialSolutionsArray = fillCandidateEdges(initialRelationshipCandidates);

//        System.out.println("Initial partial solutions:");
//        System.out.println(Arrays.toString(possiblePartialSolutionsArray));

        CLBuffer<Integer>
                possiblePartialSolutions = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(possiblePartialSolutionsArray), true);

        visitedQueryEdges.add(minCandidatesQueryEdgeId);
        visitedQueryVertices.add(initialRelationshipCandidates.getQueryStartNodeId());
        visitedQueryVertices.add(initialRelationshipCandidates.getQueryEndNodeId());

        return possiblePartialSolutions;
    }

    private int[] fillCandidateEdges(RelationshipCandidates relationshipCandidates) {
        int solutionNodeCount = this.queryContext.queryGraph.nodes.size();
        int partialSolutionCount = relationshipCandidates.getTotalCount();
        int[] possiblePartialSolutionsArray = new int[solutionNodeCount*partialSolutionCount];
        Arrays.fill(possiblePartialSolutionsArray, -1);

        Pointer<Integer> candidateStartNodes = relationshipCandidates.getCandidateStartNodes().read(this.queryKernels.queue);
        Pointer<Integer> candidateEndNodes = relationshipCandidates.getCandidateEndNodes().read(this.queryKernels.queue);
        Pointer<Integer> candidateEndNodeIndicies = relationshipCandidates.getCandidateEndNodeIndices().read(this.queryKernels.queue);
        int addedPartialSolutions = 0;
        int indexOffset = 0;
        while(addedPartialSolutions < relationshipCandidates.getTotalCount()) {


            for(int i = 0; i < relationshipCandidates.getCandidateStartNodes().getElementCount(); i++) {

                int startNode = candidateStartNodes.get(i);
                int maxIndex = candidateEndNodeIndicies.get(i+1);

                for(int j = candidateEndNodeIndicies.get(i); j < maxIndex; j++){
                    int endNode = candidateEndNodes.get(j);
                    int solutionStartNodeIndex = indexOffset + relationshipCandidates.getQueryStartNodeId();
                    int solutionEndNodeIndex = indexOffset + relationshipCandidates.getQueryEndNodeId();
                    possiblePartialSolutionsArray[solutionStartNodeIndex] = startNode;
                    possiblePartialSolutionsArray[solutionEndNodeIndex] = endNode;

                    addedPartialSolutions++;
                    indexOffset = addedPartialSolutions*solutionNodeCount;
                }
            }
        }
        return possiblePartialSolutionsArray;
    }

    private int findUnvisitedQueryEdgeId(HashMap<Integer, RelationshipCandidates> edgeCandidatesHashMap, ArrayList<Integer> visitedQueryEdges) {
        int minimumId = -1;
        int minimumCandidateCount = Integer.MAX_VALUE;
        for(int relationshipId : edgeCandidatesHashMap.keySet()) {
            RelationshipCandidates relationshipCandidates = edgeCandidatesHashMap.get(relationshipId);
            int candidateCount = relationshipCandidates.getTotalCount();
            if(candidateCount <= minimumCandidateCount) {
                minimumCandidateCount = candidateCount;
                minimumId = relationshipId;
            }
        }
        return minimumId;
    }



//    private void printEdgeCandidate(RelationshipCandidates edgeCandidates) {
//        StringBuilder builder = new StringBuilder();
//        builder.append("-----Edge candidates for query edge (");
//        builder.append(edgeCandidates.getQueryStartNodeId());
//        builder.append(", ");
//        builder.append(edgeCandidates.getQueryEndNodeId());
//        builder.append(")-----\n");
//
//        Pointer<Integer> startNodesPointer = edgeCandidates.getCandidateStartNodes().read(this.queryKernels.queue);
//        Pointer<Integer> endNodeIndiciesPointer = edgeCandidates.getCandidateEndNodeIndices().read(this.queryKernels.queue);
//        Pointer<Integer> endNodesPointer = edgeCandidates.getCandidateEndNodes().read(this.queryKernels.queue);
//
//        builder.append("Start nodes: [");
//        for(int i = 0; i < edgeCandidates.getStartNodeCount(); i++) {
//            builder.append(startNodesPointer.get(i) + ", ");
//        }
//
//        builder.append("]\n");
//
//        builder.append("End node indicies: [");
//        for(int i = 0; i < edgeCandidates.getStartNodeCount()+1; i++) {
//            builder.append(endNodeIndiciesPointer.get(i) + ", ");
//        }
//
//        builder.append("]\n");
//
//        builder.append("End nodes: [");
//        for(int i = 0; i < edgeCandidates.getTotalCount(); i++) {
//            builder.append(endNodesPointer.get(i) + ", ");
//        }
//        builder.append("]\n");
//
//        System.out.println(builder.toString());
//    }

}
