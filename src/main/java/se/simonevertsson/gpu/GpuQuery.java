package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.*;
import org.bridj.Pointer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

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
        createCandidateIndicatorsBuffer(queryContext.dataNodeCount, queryContext.queryNodeCount);
    }

    public void executeQuery(ArrayList<Node> visitOrder) throws IOException {

        /****** Candidate initialization step ******/
        CandidateInitializer candidateInitializer =
                new CandidateInitializer(this.queryContext, this.queryKernels, this.bufferContainer);
        candidateInitializer.candidateInitialization(visitOrder);


//        System.out.println("--------------REFINEMENT STEP-------------");
        candidateRefinement(visitOrder);

//        System.out.println("--------------FINDING CANDIDATE EDGES-------------");
        HashMap<Integer, EdgeCandidates> edgeCandidatesHashMap =  candidateEdgeSearch();

//        System.out.println("--------------JOINING CANDIDATE EDGES-------------");
        candidateEdgeJoin(edgeCandidatesHashMap);
    }

    private void candidateEdgeJoin(HashMap<Integer, EdgeCandidates> edgeCandidatesHashMap) throws IOException {
        ArrayList<Integer> visitedQueryEdges = new ArrayList<Integer>();
        ArrayList<Integer> visitedQueryVertices = new ArrayList<Integer>();

        CLBuffer<Integer> possibleSolutions = initializePossiblePartialSolutions(edgeCandidatesHashMap, visitedQueryEdges, visitedQueryVertices);

        for(int relationshipId : edgeCandidatesHashMap.keySet()) {
            if(!visitedQueryEdges.contains(relationshipId)) {
                EdgeCandidates edgeCandidates = edgeCandidatesHashMap.get(relationshipId);
                int startNodeId = edgeCandidates.getQueryStartNodeId();
                int endNodeId = edgeCandidates.getQueryEndNodeId();
                boolean startNodeVisisted = visitedQueryVertices.contains(startNodeId);
                boolean endNodeVisisted = visitedQueryVertices.contains(endNodeId);

                int possibleSolutionCount = (int)possibleSolutions.getElementCount()/ this.queryContext.queryNodeCount;

                CLBuffer<Integer>
                        combinationCounts = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, possibleSolutionCount);

                if(startNodeVisisted && endNodeVisisted) {
                    /* Prune existing possible solutions */
                    CLBuffer<Boolean> validationIndicators = validateSolutions(possibleSolutions, possibleSolutionCount, startNodeId, endNodeId, edgeCandidates);

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
                            edgeCandidates, startNodeId,
                            endNodeId, startNodeVisisted,
                            possibleSolutionCount,
                            combinationCounts);

                    int[] combinationIndicies = generatePrefixScanArray(combinationCountsPointer, possibleSolutionCount);

                    CLBuffer<Integer> newPossibleSolutions = generateSolutionCombinations(
                            possibleSolutions,
                            possibleSolutionCount,
                            edgeCandidates,
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

    private CLBuffer<Boolean> validateSolutions(CLBuffer<Integer> possibleSolutions, int possibleSolutionCount, int startNodeId, int endNodeId, EdgeCandidates edgeCandidates) throws IOException {
        CLBuffer<Boolean> validationIndicators = this.queryKernels.context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToBooleans(new boolean[possibleSolutionCount]));

        int[] globalSizes = new int[] { possibleSolutionCount };

        CLEvent validateSolutionsEvent = this.queryKernels.validateSolutionsKernel.validate_solutions(
                this.queryKernels.queue,
                startNodeId,
                endNodeId,
                possibleSolutions,
                edgeCandidates.getCandidateStartNodes(),
                edgeCandidates.getCandidateEndNodeIndicies(),
                edgeCandidates.getCandidateEndNodes(),
                edgeCandidates.getStartNodeCount(),
                validationIndicators,
                globalSizes,
                null
        );

        validateSolutionsEvent.waitFor();

        return validationIndicators;
    }

    private CLBuffer<Integer> generateSolutionCombinations(CLBuffer<Integer> oldPossibleSolutions, int oldPossibleSolutionCount, EdgeCandidates edgeCandidates, int startNodeId, int endNodeId, boolean startNodeVisisted, int[] combinationIndicies) throws IOException {
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
                edgeCandidates.getCandidateStartNodes(),
                edgeCandidates.getCandidateEndNodeIndicies(),
                edgeCandidates.getCandidateEndNodes(),
                startNodeVisisted,
                edgeCandidates.getStartNodeCount(),
                possibleSolutions,
                globalSizes,
                null
        );
        generateSolutionCombinationsEvent.waitFor();

        return possibleSolutions;
    }

    private Pointer<Integer> countSolutionCombinations(CLBuffer<Integer> possiblePartialSolutions, EdgeCandidates edgeCandidates, int startNodeId, int endNodeId, boolean startNodeVisisted, int combinationCountsLength, CLBuffer<Integer> combinationCounts) throws IOException {
        int[] globalSizes = new int[] { combinationCountsLength };
        CLEvent countSolutionCombinationsEvent = this.queryKernels.countSolutionCombinationsKernel.count_solution_combinations(
                this.queryKernels.queue,
                startNodeId,
                endNodeId,
                possiblePartialSolutions,
                edgeCandidates.getCandidateStartNodes(),
                edgeCandidates.getCandidateEndNodeIndicies(),
                edgeCandidates.getCandidateEndNodes(),
                startNodeVisisted,
                edgeCandidates.getStartNodeCount(),
                combinationCounts,
                globalSizes,
                null
        );

        return combinationCounts.read(this.queryKernels.queue, countSolutionCombinationsEvent);
    }

    private CLBuffer<Integer> initializePossiblePartialSolutions(HashMap<Integer, EdgeCandidates> edgeCandidatesHashMap, ArrayList<Integer> visitedQueryEdges, ArrayList<Integer> visitedQueryVertices) {
        int minCandidatesQueryEdgeId = findUnvisitedQueryEdgeId(edgeCandidatesHashMap, visitedQueryEdges);
        EdgeCandidates initialEdgeCandidates = edgeCandidatesHashMap.get(minCandidatesQueryEdgeId);

        int solutionSize = this.queryContext.queryGraph.nodes.size();
        int initalPartialSolutionCount = initialEdgeCandidates.getCount();
        int[] possiblePartialSolutionsArray = fillCandidateEdges(initialEdgeCandidates);

//        System.out.println("Initial partial solutions:");
//        System.out.println(Arrays.toString(possiblePartialSolutionsArray));

        CLBuffer<Integer>
                possiblePartialSolutions = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(possiblePartialSolutionsArray), true);

        visitedQueryEdges.add(minCandidatesQueryEdgeId);
        visitedQueryVertices.add(initialEdgeCandidates.getQueryStartNodeId());
        visitedQueryVertices.add(initialEdgeCandidates.getQueryEndNodeId());

        return possiblePartialSolutions;
    }

    private int[] fillCandidateEdges(EdgeCandidates edgeCandidates) {
        int solutionNodeCount = this.queryContext.queryGraph.nodes.size();
        int partialSolutionCount = edgeCandidates.getCount();
        int[] possiblePartialSolutionsArray = new int[solutionNodeCount*partialSolutionCount];
        Arrays.fill(possiblePartialSolutionsArray, -1);

        Pointer<Integer> candidateStartNodes = edgeCandidates.getCandidateStartNodes().read(this.queryKernels.queue);
        Pointer<Integer> candidateEndNodes = edgeCandidates.getCandidateEndNodes().read(this.queryKernels.queue);
        Pointer<Integer> candidateEndNodeIndicies = edgeCandidates.getCandidateEndNodeIndicies().read(this.queryKernels.queue);
        int addedPartialSolutions = 0;
        int indexOffset = 0;
        while(addedPartialSolutions < edgeCandidates.getCount()) {


            for(int i = 0; i < edgeCandidates.getCandidateStartNodes().getElementCount(); i++) {

                int startNode = candidateStartNodes.get(i);
                int maxIndex = candidateEndNodeIndicies.get(i+1);

                for(int j = candidateEndNodeIndicies.get(i); j < maxIndex; j++){
                    int endNode = candidateEndNodes.get(j);
                    int solutionStartNodeIndex = indexOffset + edgeCandidates.getQueryStartNodeId();
                    int solutionEndNodeIndex = indexOffset + edgeCandidates.getQueryEndNodeId();
                    possiblePartialSolutionsArray[solutionStartNodeIndex] = startNode;
                    possiblePartialSolutionsArray[solutionEndNodeIndex] = endNode;

                    addedPartialSolutions++;
                    indexOffset = addedPartialSolutions*solutionNodeCount;
                }
            }
        }
        return possiblePartialSolutionsArray;
    }

    private int findUnvisitedQueryEdgeId(HashMap<Integer, EdgeCandidates> edgeCandidatesHashMap, ArrayList<Integer> visitedQueryEdges) {
        int minimumId = -1;
        int minimumCandidateCount = Integer.MAX_VALUE;
        for(int relationshipId : edgeCandidatesHashMap.keySet()) {
            EdgeCandidates edgeCandidates = edgeCandidatesHashMap.get(relationshipId);
            int candidateCount = edgeCandidates.getCount();
            if(candidateCount <= minimumCandidateCount) {
                minimumCandidateCount = candidateCount;
                minimumId = relationshipId;
            }
        }
        return minimumId;
    }

    private HashMap<Integer, EdgeCandidates> candidateEdgeSearch() throws IOException {
        ArrayList<Relationship> relationships = this.queryContext.queryGraph.relationships;
        HashMap<Integer, EdgeCandidates> edgeCandidatesHashMap = new HashMap<Integer, EdgeCandidates>();

        for(Relationship relationship : relationships) {
            int queryStartNodeId = (int) relationship.getStartNode().getId();
            int queryEndNodeId = (int) relationship.getEndNode().getId();
            int[] candidateArray = gatherCandidateArray((int) relationship.getStartNode().getId());
            CLBuffer<Integer>
                    candidatesArray = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateArray), true);

            Pointer<Integer> candidateEdgeCountsPointer = countEdgeCandidates(queryStartNodeId, queryEndNodeId, candidatesArray);

            int candidateArrayLength = candidateArray.length;

            int[] candidateEdgeEndNodeIndicies = generatePrefixScanArray(candidateEdgeCountsPointer, candidateArrayLength);

            CLBuffer<Integer>
                    candidateEdgeEndNodeIndiciesBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateEdgeEndNodeIndicies), true);

            CLBuffer<Integer> candidateEdgeEndNodes =
                    searchCandidateEdges(queryStartNodeId, queryEndNodeId, candidatesArray, candidateEdgeEndNodeIndiciesBuffer, candidateEdgeEndNodeIndicies[candidateArray.length]);

            EdgeCandidates edgeCandidates = new EdgeCandidates(
                    queryStartNodeId,
                    queryEndNodeId,
                    candidateEdgeEndNodeIndiciesBuffer,
                    candidateEdgeEndNodes,
                    candidatesArray);

//            printEdgeCandidate(edgeCandidates);

            edgeCandidatesHashMap.put((int) relationship.getId(), edgeCandidates);

        }

        return edgeCandidatesHashMap;
    }

    private int[] generatePrefixScanArray(Pointer<Integer> bufferPointer, int bufferSize) {
        int totalElementCount = 0;
        int[] prefixScanArray = new int[bufferSize +1];
        for(int i = 0; i < bufferSize; i++) {
            prefixScanArray[i] = totalElementCount;
            totalElementCount += bufferPointer.get(i);
        }
        prefixScanArray[bufferSize] = totalElementCount;
        return prefixScanArray;
    }

    private void printEdgeCandidate(EdgeCandidates edgeCandidates) {
        StringBuilder builder = new StringBuilder();
        builder.append("-----Edge candidates for query edge (");
        builder.append(edgeCandidates.getQueryStartNodeId());
        builder.append(", ");
        builder.append(edgeCandidates.getQueryEndNodeId());
        builder.append(")-----\n");

        Pointer<Integer> startNodesPointer = edgeCandidates.getCandidateStartNodes().read(this.queryKernels.queue);
        Pointer<Integer> endNodeIndiciesPointer = edgeCandidates.getCandidateEndNodeIndicies().read(this.queryKernels.queue);
        Pointer<Integer> endNodesPointer = edgeCandidates.getCandidateEndNodes().read(this.queryKernels.queue);

        builder.append("Start nodes: [");
        for(int i = 0; i < edgeCandidates.getStartNodeCount(); i++) {
            builder.append(startNodesPointer.get(i) + ", ");
        }

        builder.append("]\n");

        builder.append("End node indicies: [");
        for(int i = 0; i < edgeCandidates.getStartNodeCount()+1; i++) {
            builder.append(endNodeIndiciesPointer.get(i) + ", ");
        }

        builder.append("]\n");

        builder.append("End nodes: [");
        for(int i = 0; i < edgeCandidates.getCount(); i++) {
            builder.append(endNodesPointer.get(i) + ", ");
        }
        builder.append("]\n");

        System.out.println(builder.toString());
    }


    private CLBuffer<Integer> searchCandidateEdges(int queryStartNodeId, int queryEndNodeId, CLBuffer<Integer> candidatesArray,
                                                  CLBuffer<Integer> candidateEdgeEndNodeIndicies, int totalCandidateEdgeCount) throws IOException {

        CLBuffer candidateEdgeEndNodes = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, totalCandidateEdgeCount);


        int[] globalSizes = new int[] {(int) candidatesArray.getElementCount()};

        CLEvent searchEdgeCandidatesEvent = this.queryKernels.searchEdgeCandidatesKernel.count_edge_candidates(
                this.queryKernels.queue,
                queryStartNodeId,
                queryEndNodeId,
                this.bufferContainer.dataBuffers.dataNodeRelationshipsBuffer,
                this.bufferContainer.dataBuffers.dataRelationshipIndicesBuffer,
                candidateEdgeEndNodes,
                candidateEdgeEndNodeIndicies,
                candidatesArray,
                this.bufferContainer.queryBuffers.candidateIndicatorsBuffer,
                this.queryContext.dataNodeCount,
                globalSizes,
                null
        );

        searchEdgeCandidatesEvent.waitFor();
        return candidateEdgeEndNodes;
    }


    private Pointer<Integer> countEdgeCandidates(int queryStartNodeId, int queryEndNodeId, CLBuffer<Integer> candidateArray) throws IOException {

        CLBuffer candidateEdgeCounts = this.queryKernels.context.createIntBuffer(CLMem.Usage.Output, candidateArray.getElementCount());


        int[] globalSizes = new int[] {(int) candidateArray.getElementCount()};

        CLEvent countEdgeCandidatesEvent = this.queryKernels.countEdgeCandidatesKernel.count_edge_candidates(
                this.queryKernels.queue,
                queryStartNodeId,
                queryEndNodeId,
                this.bufferContainer.dataBuffers.dataNodeRelationshipsBuffer,
                this.bufferContainer.dataBuffers.dataRelationshipIndicesBuffer,
                candidateEdgeCounts,
                candidateArray,
                this.bufferContainer.queryBuffers.candidateIndicatorsBuffer,
                this.queryContext.dataNodeCount,
                globalSizes,
                null
        );


        return candidateEdgeCounts.read(this.queryKernels.queue, countEdgeCandidatesEvent);
    }

    private void candidateRefinement(ArrayList<Node> visitOrder) throws IOException {
        boolean[] oldCandidateIndicators = pointerToArray(this.bufferContainer.queryBuffers.candidateIndicatorsPointer, this.queryContext.dataNodeCount * this.queryContext.queryNodeCount);
        boolean candidateIndicatorsHasChanged = true;
        while(candidateIndicatorsHasChanged) {
//            System.out.println("Candidate indicators have been updated, refining again.");
            for (Node queryNode : visitOrder) {
                int[] candidateArray = gatherCandidateArray((int) queryNode.getId());
                if(candidateArray.length > 0) {
                    refineCandidates((int) queryNode.getId(), candidateArray);
                } else {
                    throw new IllegalStateException("Candidate refinement yielded no candidates for query node " + queryNode.getId());
                }
            }

            boolean[] newCandidateIndicators = pointerToArray(this.bufferContainer.queryBuffers.candidateIndicatorsPointer, this.queryContext.dataNodeCount * this.queryContext.queryNodeCount);
            candidateIndicatorsHasChanged = !Arrays.equals(oldCandidateIndicators, newCandidateIndicators);
            oldCandidateIndicators = newCandidateIndicators;
        }
    }

    private boolean[] pointerToArray(Pointer<Boolean> pointer, int size) {
        boolean[] result = new boolean[size];
        int i = 0;
        for(boolean element : pointer) {
            result[i] = element;
            i++;
        }
        return result;
    }

    private void printCandidateIndicatorMatrix() {
        StringBuilder builder = new StringBuilder();
        int j = 1;
        for(boolean candidate : bufferContainer.queryBuffers.candidateIndicatorsPointer)  {
            builder.append(candidate + ", ");
            if(j % queryContext.dataNodeCount == 0) {
                builder.append("\n");
            }
            j++;
        }
        System.out.println(builder.toString());
    }

    private DataBuffers createDataBuffers(GpuGraphModel data) {
        IntBuffer dataAdjacenciesBuffer = IntBuffer.wrap(data.getNodeRelationships());
        IntBuffer dataAdjacencyIndexBuffer = IntBuffer.wrap(data.getRelationshipIndices());
        IntBuffer dataLabelsBuffer = IntBuffer.wrap(data.getNodeLabels());
        IntBuffer dataLabelIndexBuffer = IntBuffer.wrap(data.getLabelIndices());

        DataBuffers dataBuffers = new DataBuffers();

        dataBuffers.dataNodeRelationshipsBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataAdjacenciesBuffer, true);
        dataBuffers.dataRelationshipIndicesBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataAdjacencyIndexBuffer, true);
        dataBuffers.dataLabelsBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataLabelsBuffer, true);
        dataBuffers.dataLabelIndicesBuffer = queryKernels.context.createIntBuffer(CLMem.Usage.Input, dataLabelIndexBuffer, true);


        return dataBuffers;
    }


    private QueryBuffers createQueryBuffers(GpuGraphModel query) {

        IntBuffer queryNodeAdjacenciesBuffer = IntBuffer.wrap(query.getNodeRelationships());
        IntBuffer queryNodeAdjacencyIndiciesBuffer = IntBuffer.wrap(query.getRelationshipIndices());
        IntBuffer queryNodeLabelsBuffer = IntBuffer.wrap(query.getNodeLabels());
        IntBuffer queryNodeLabelIndiciesBuffer = IntBuffer.wrap(query.getLabelIndices());

        QueryBuffers queryBuffers = new QueryBuffers();

        queryBuffers.queryNodeLabelsBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelsBuffer, true);
        queryBuffers.queryNodeLabelIndicesBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelIndiciesBuffer, true);
        queryBuffers.queryNodeRelationshipsBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacenciesBuffer, true);
        queryBuffers.queryRelationshipIndicesBuffer = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacencyIndiciesBuffer, true);

        return queryBuffers;
    }

    private void createCandidateIndicatorsBuffer(int dataNodeCount, int queryNodeCount) {
        boolean candidateIndicators[] = new boolean[dataNodeCount * queryNodeCount];

        this.bufferContainer.queryBuffers.candidateIndicatorsBuffer = queryKernels.context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToBooleans(candidateIndicators), true);
    }

    public int[] gatherCandidateArray(int nodeId) {
        return QueryUtils.gatherCandidateArray(this.bufferContainer.queryBuffers.candidateIndicatorsPointer, this.queryContext.dataNodeCount, nodeId);
    }


    private void refineCandidates(int queryNodeId, int[] candidateArray) throws IOException {
        int queryNodeAdjacencyIndexStart = queryContext.gpuQuery.getRelationshipIndices()[queryNodeId];
        int queryNodeAdjacencyIndexEnd = queryContext.gpuQuery.getRelationshipIndices()[queryNodeId + 1];

        IntBuffer candidatesArrayBuffer = IntBuffer.wrap(candidateArray);
        CLBuffer<Integer> candidatesArray
                = this.queryKernels.context.createIntBuffer(CLMem.Usage.Input, candidatesArrayBuffer, true);
        int[] globalSizes = new int[] { candidateArray.length };

        CLEvent refineCandidatesEvent = this.queryKernels.refineCandidatesKernel.refine_candidates(
                queryKernels.queue,
                queryNodeId,
                bufferContainer.queryBuffers.queryNodeRelationshipsBuffer,
                bufferContainer.queryBuffers.queryRelationshipIndicesBuffer,
                bufferContainer.queryBuffers.queryNodeLabelsBuffer,
                bufferContainer.queryBuffers.queryNodeLabelIndicesBuffer,
                queryNodeAdjacencyIndexStart,
                queryNodeAdjacencyIndexEnd,
                bufferContainer.dataBuffers.dataNodeRelationshipsBuffer,
                bufferContainer.dataBuffers.dataRelationshipIndicesBuffer,
                bufferContainer.dataBuffers.dataLabelsBuffer,
                bufferContainer.dataBuffers.dataLabelIndicesBuffer,
                candidatesArray,
                bufferContainer.queryBuffers.candidateIndicatorsBuffer,
                this.queryContext.dataNodeCount,
                globalSizes,
                null
        );

        this.bufferContainer.queryBuffers.candidateIndicatorsPointer = bufferContainer.queryBuffers.candidateIndicatorsBuffer.read(this.queryKernels.queue, refineCandidatesEvent);
    }
}
