package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.*;
import org.bridj.Pointer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.*;
import se.simonevertsson.query.QueryGraph;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class GpuQuery {

    private final CLContext context;
    private final CLQueue queue;
    private final GpuGraphModel gpuData;
    private final GpuGraphModel gpuQuery;
    private final int queryNodeCount;
    private final int dataNodeCount;
    private final QueryGraph queryGraph;
    private final CheckCandidates checkCandidatesKernel;
    private final ExploreCandidates exploreCandidatesKernel;
    private final RefineCandidates refineCandidatesKernel;
    private final CountEdgeCandidates countEdgeCandidatesKernel;
    private final SearchEdgeCandidates searchEdgeCandidatesKernel;
    private final CountSolutionCombinations countSolutionCombinationsKernel;
    private final GenerateSolutionCombinations generateSolutionCombinationsKernel;
    private final ValidateSolutions validateSolutionsKernel;
    private final PruneSolutions pruneSolutionsKernel;
    private CLBuffer<Integer> dataAdjacencyIndices;
    private CLBuffer<Integer> dataLabels;
    private CLBuffer<Integer> dataLabelIndices;
    private CLBuffer<Boolean> candidateIndicators;
    private CLBuffer<Integer> dataAdjacences;
    private CLBuffer<Integer> queryNodeLabels;
    private CLBuffer<Integer> queryNodeLabelIndices;
    private CLBuffer<Integer> queryNodeAdjacencies;
    private CLBuffer<Integer> queryNodeAdjacencyIndices;
    private boolean[] initializedQueryNode;
    private Pointer<Boolean> candidateIndicatorsPointer;


    public GpuQuery(GpuGraphModel gpuData, GpuGraphModel gpuQuery, QueryGraph queryGraph) throws IOException {
        this.gpuData = gpuData;
        this.gpuQuery = gpuQuery;
        this.queryGraph = queryGraph;

        this.context = JavaCL.createBestContext();
        this.queue = this.context.createDefaultQueue();
        this.checkCandidatesKernel = new CheckCandidates(this.context);
        this.exploreCandidatesKernel = new ExploreCandidates(this.context);
        this.refineCandidatesKernel = new RefineCandidates(this.context);
        this.countEdgeCandidatesKernel = new CountEdgeCandidates(this.context);
        this.searchEdgeCandidatesKernel = new SearchEdgeCandidates(this.context);
        this.countSolutionCombinationsKernel = new CountSolutionCombinations(this.context);
        this.generateSolutionCombinationsKernel = new GenerateSolutionCombinations(this.context);
        this.validateSolutionsKernel = new ValidateSolutions(this.context);
        this.pruneSolutionsKernel = new PruneSolutions(this.context);

        this.dataNodeCount = gpuData.getNodeLabels().length;
        this.queryNodeCount = gpuQuery.getNodeLabels().length;
        this.initializedQueryNode = new boolean[queryNodeCount];
    }

    public void executeQuery(ArrayList<Node> visitOrder) throws IOException {
        createDataBuffers(dataNodeCount, queryNodeCount);

        candidateInitialization(visitOrder);

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

                int possibleSolutionCount = (int)possibleSolutions.getElementCount()/this.queryNodeCount;

                CLBuffer<Integer>
                        combinationCounts = this.context.createIntBuffer(CLMem.Usage.Output, possibleSolutionCount);

                if(startNodeVisisted && endNodeVisisted) {
                    /* Prune existing possible solutions */
                    CLBuffer<Boolean> validationIndicators = validateSolutions(possibleSolutions, possibleSolutionCount, startNodeId, endNodeId, edgeCandidates);

                    Pointer<Boolean> validationIndicatorsPointer = validationIndicators.read(this.queue);
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

                    Pointer<Integer> prunedPossibleSolutionsPointer = prunedPossibleSolutions.read(this.queue);

                    int result[] = new int[validSolutionCount*this.queryNodeCount];
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

                    Pointer<Integer> newPossibleSolutionsPointer = newPossibleSolutions.read(this.queue);

                    int result[] = new int[combinationIndicies[combinationIndicies.length-1]*this.queryNodeCount];
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
        Pointer<Integer> solutionsPointer = solutionsBuffer.read(this.queue);
        int solutionCount = (int) (solutionsBuffer.getElementCount()/this.queryNodeCount);

        StringBuilder builder = new StringBuilder();
        builder.append("Final solutions:\n");

        for(int i = 0; i < solutionCount*queryNodeCount; i++) {
            if(i % this.queryNodeCount == 0) {
                builder.append("(");
                builder.append(solutionsPointer.get(i));
            } else {
                builder.append(", ");
                builder.append(solutionsPointer.get(i));
                if (i % this.queryNodeCount == this.queryNodeCount - 1) {
                    builder.append(")\n");
                }
            }
        }

        System.out.println(builder.toString());
    }

    private CLBuffer<Integer> prunePossibleSolutions(CLBuffer<Integer> oldPossibleSolutions, int possibleSolutionCount, CLBuffer<Boolean> validationIndicators, int[] outputIndexArray) throws IOException {
        CLBuffer<Integer> outputIndices = this.context.createIntBuffer(
                CLMem.Usage.Input,
                IntBuffer.wrap(outputIndexArray),
                true);

        CLBuffer<Integer> prunedPossibleSolutions = this.context.createIntBuffer(
                CLMem.Usage.Input,
                outputIndexArray[outputIndexArray.length-1]*this.queryNodeCount
                );

        int[] globalSizes = new int[] { possibleSolutionCount };

        CLEvent pruneSolutionsEvent = this.pruneSolutionsKernel.prune_solutions(
                this.queue,
                this.queryNodeCount,
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
        CLBuffer<Boolean> validationIndicators = this.context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToBooleans(new boolean[possibleSolutionCount]));

        int[] globalSizes = new int[] { possibleSolutionCount };

        CLEvent validateSolutionsEvent = this.validateSolutionsKernel.validate_solutions(
                this.queue,
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
                combinationIndicesBuffer = this.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(combinationIndicies), true),
                possibleSolutions = this.context.createIntBuffer(CLMem.Usage.Output, totalCombinationCount * this.queryNodeCount);

        CLEvent generateSolutionCombinationsEvent = this.generateSolutionCombinationsKernel.generate_solution_combinations(
                this.queue,
                startNodeId,
                endNodeId,
                this.queryNodeCount,
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
        CLEvent countSolutionCombinationsEvent = this.countSolutionCombinationsKernel.count_solution_combinations(
                this.queue,
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

        return combinationCounts.read(this.queue, countSolutionCombinationsEvent);
    }

    private CLBuffer<Integer> initializePossiblePartialSolutions(HashMap<Integer, EdgeCandidates> edgeCandidatesHashMap, ArrayList<Integer> visitedQueryEdges, ArrayList<Integer> visitedQueryVertices) {
        int minCandidatesQueryEdgeId = findUnvisitedQueryEdgeId(edgeCandidatesHashMap, visitedQueryEdges);
        EdgeCandidates initialEdgeCandidates = edgeCandidatesHashMap.get(minCandidatesQueryEdgeId);

        int solutionSize = this.queryGraph.nodes.size();
        int initalPartialSolutionCount = initialEdgeCandidates.getCount();
        int[] possiblePartialSolutionsArray = fillCandidateEdges(initialEdgeCandidates);

//        System.out.println("Initial partial solutions:");
//        System.out.println(Arrays.toString(possiblePartialSolutionsArray));

        CLBuffer<Integer>
                possiblePartialSolutions = this.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(possiblePartialSolutionsArray), true);

        visitedQueryEdges.add(minCandidatesQueryEdgeId);
        visitedQueryVertices.add(initialEdgeCandidates.getQueryStartNodeId());
        visitedQueryVertices.add(initialEdgeCandidates.getQueryEndNodeId());

        return possiblePartialSolutions;
    }

    private int[] fillCandidateEdges(EdgeCandidates edgeCandidates) {
        int solutionNodeCount = this.queryGraph.nodes.size();
        int partialSolutionCount = edgeCandidates.getCount();
        int[] possiblePartialSolutionsArray = new int[solutionNodeCount*partialSolutionCount];
        Arrays.fill(possiblePartialSolutionsArray, -1);

        Pointer<Integer> candidateStartNodes = edgeCandidates.getCandidateStartNodes().read(this.queue);
        Pointer<Integer> candidateEndNodes = edgeCandidates.getCandidateEndNodes().read(this.queue);
        Pointer<Integer> candidateEndNodeIndicies = edgeCandidates.getCandidateEndNodeIndicies().read(this.queue);
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
        ArrayList<Relationship> relationships = this.queryGraph.relationships;
        HashMap<Integer, EdgeCandidates> edgeCandidatesHashMap = new HashMap<Integer, EdgeCandidates>();

        for(Relationship relationship : relationships) {
            int queryStartNodeId = (int) relationship.getStartNode().getId();
            int queryEndNodeId = (int) relationship.getEndNode().getId();
            int[] candidateArray = gatherCandidateArray((int) relationship.getStartNode().getId());
            CLBuffer<Integer>
                    candidatesArray = this.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateArray), true);

            Pointer<Integer> candidateEdgeCountsPointer = countEdgeCandidates(queryStartNodeId, queryEndNodeId, candidatesArray);

            int candidateArrayLength = candidateArray.length;

            int[] candidateEdgeEndNodeIndicies = generatePrefixScanArray(candidateEdgeCountsPointer, candidateArrayLength);

            CLBuffer<Integer>
                    candidateEdgeEndNodeIndiciesBuffer = this.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateEdgeEndNodeIndicies), true);

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

        Pointer<Integer> startNodesPointer = edgeCandidates.getCandidateStartNodes().read(this.queue);
        Pointer<Integer> endNodeIndiciesPointer = edgeCandidates.getCandidateEndNodeIndicies().read(this.queue);
        Pointer<Integer> endNodesPointer = edgeCandidates.getCandidateEndNodes().read(this.queue);

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

        CLBuffer candidateEdgeEndNodes = this.context.createIntBuffer(CLMem.Usage.Output, totalCandidateEdgeCount);


        int[] globalSizes = new int[] {(int) candidatesArray.getElementCount()};

        CLEvent searchEdgeCandidatesEvent = this.searchEdgeCandidatesKernel.count_edge_candidates(
                this.queue,
                queryStartNodeId,
                queryEndNodeId,
                this.dataAdjacences,
                this.dataAdjacencyIndices,
                candidateEdgeEndNodes,
                candidateEdgeEndNodeIndicies,
                candidatesArray,
                this.candidateIndicators,
                this.dataNodeCount,
                globalSizes,
                null
        );

        searchEdgeCandidatesEvent.waitFor();
        return candidateEdgeEndNodes;
    }


    private Pointer<Integer> countEdgeCandidates(int queryStartNodeId, int queryEndNodeId, CLBuffer<Integer> candidateArray) throws IOException {

        CLBuffer candidateEdgeCounts = this.context.createIntBuffer(CLMem.Usage.Output, candidateArray.getElementCount());


        int[] globalSizes = new int[] {(int) candidateArray.getElementCount()};

        CLEvent countEdgeCandidatesEvent = this.countEdgeCandidatesKernel.count_edge_candidates(
                this.queue,
                queryStartNodeId,
                queryEndNodeId,
                this.dataAdjacences,
                this.dataAdjacencyIndices,
                candidateEdgeCounts,
                candidateArray,
                this.candidateIndicators,
                this.dataNodeCount,
                globalSizes,
                null
        );


        return candidateEdgeCounts.read(this.queue, countEdgeCandidatesEvent);
    }

    private void candidateRefinement(ArrayList<Node> visitOrder) throws IOException {
        boolean[] oldCandidateIndicators = pointerToArray(this.candidateIndicatorsPointer, this.dataNodeCount * this.queryNodeCount);
        boolean candidateIndicatorsHasChanged = true;
        while(candidateIndicatorsHasChanged) {
//            System.out.println("Candidate indicators have been updated, refining again.");
            for (Node queryNode : visitOrder) {
                int[] candidateArray = gatherCandidateArray((int) queryNode.getId());
                refineCandidates((int) queryNode.getId(), candidateArray);
            }

            boolean[] newCandidateIndicators = pointerToArray(this.candidateIndicatorsPointer, this.dataNodeCount*this.queryNodeCount);
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

    private void candidateInitialization(ArrayList<Node> visitOrder) throws IOException {
        for(Node queryNode : visitOrder) {
            if(!initializedQueryNode[((int) queryNode.getId())]) {
                checkCandidates(gpuQuery, queryNode);
//                printCandidateIndicatorMatrix();
            }
            int[] candidateArray = gatherCandidateArray((int) queryNode.getId());
//            System.out.println("Candidate array for query node " + queryNode.getId() + ": " + Arrays.toString(candidateArray));
            exploreCandidates((int) queryNode.getId(), candidateArray);
//            printCandidateIndicatorMatrix();
        }
    }

    private void printCandidateIndicatorMatrix() {
        StringBuilder builder = new StringBuilder();
        int j = 1;
        for(boolean candidate : candidateIndicatorsPointer)  {
            builder.append(candidate + ", ");
            if(j % dataNodeCount == 0) {
                builder.append("\n");
            }
            j++;
        }
        System.out.println(builder.toString());
    }

    private void exploreCandidates(int queryNodeId, int[] candidateArray) throws IOException {
        int queryNodeAdjacencyIndexStart = gpuQuery.getAdjacencyIndicies()[queryNodeId];
        int queryNodeAdjacencyIndexEnd = gpuQuery.getAdjacencyIndicies()[queryNodeId + 1];

        IntBuffer candidatesArrayBuffer = IntBuffer.wrap(candidateArray);
        CLBuffer<Integer> candidatesArray
                = this.context.createIntBuffer(CLMem.Usage.Input, candidatesArrayBuffer, true);
        int[] globalSizes = new int[] { candidateArray.length };

        CLEvent exploreCandidatesEvent = this.exploreCandidatesKernel.explore_candidates(
            queue,
            queryNodeId,
            queryNodeAdjacencies,
                queryNodeAdjacencyIndices,
            queryNodeLabels,
                queryNodeLabelIndices,
            queryNodeAdjacencyIndexStart,
            queryNodeAdjacencyIndexEnd,
                dataAdjacences,
                dataAdjacencyIndices,
            dataLabels,
                dataLabelIndices,
            candidatesArray,
            candidateIndicators,
            this.dataNodeCount,
            globalSizes,
            null
        );

        this.candidateIndicatorsPointer = candidateIndicators.read(this.queue, exploreCandidatesEvent);
    }

    private void createDataBuffers(int dataNodeCount, int queryNodeCount) {
        IntBuffer queryNodeAdjacenciesBuffer = IntBuffer.wrap(gpuQuery.getNodeAdjecencies());
        IntBuffer queryNodeAdjacencyIndiciesBuffer = IntBuffer.wrap(gpuQuery.getAdjacencyIndicies());
        IntBuffer queryNodeLabelsBuffer = IntBuffer.wrap(gpuQuery.getNodeLabels());
        IntBuffer queryNodeLabelIndiciesBuffer = IntBuffer.wrap(gpuQuery.getLabelIndicies());


        IntBuffer dataAdjacenciesBuffer = IntBuffer.wrap(gpuData.getNodeAdjecencies());
        IntBuffer dataAdjacencyIndexBuffer = IntBuffer.wrap(gpuData.getAdjacencyIndicies());
        IntBuffer dataLabelsBuffer = IntBuffer.wrap(gpuData.getNodeLabels());
        IntBuffer dataLabelIndexBuffer = IntBuffer.wrap(gpuData.getLabelIndicies());


        this.queryNodeLabels = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelsBuffer, true);
        this.queryNodeLabelIndices = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelIndiciesBuffer, true);
        this.queryNodeAdjacencies = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacenciesBuffer, true);
        this.queryNodeAdjacencyIndices = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacencyIndiciesBuffer, true);

        this.dataAdjacences = this.context.createIntBuffer(CLMem.Usage.Input, dataAdjacenciesBuffer, true);
        this.dataAdjacencyIndices = this.context.createIntBuffer(CLMem.Usage.Input, dataAdjacencyIndexBuffer, true);
        this.dataLabels = context.createIntBuffer(CLMem.Usage.Input, dataLabelsBuffer, true);
        this.dataLabelIndices = context.createIntBuffer(CLMem.Usage.Input, dataLabelIndexBuffer, true);


        this.candidateIndicators = context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToBooleans(new boolean[dataNodeCount * queryNodeCount]));
    }

    public void checkCandidates(GpuGraphModel gpuQuery, Node queryNode) throws IOException {
//        System.out.println("---------------------------");
        int queryLabelStartIndex = gpuQuery.getLabelIndicies()[((int) queryNode.getId())];
        int queryLabelEndIndex = gpuQuery.getLabelIndicies()[((int) queryNode.getId())+1];
        int queryNodeDegree = queryNode.getDegree();
        int[] queryNodeLabels = Arrays.copyOfRange(gpuQuery.getNodeLabels(), queryLabelStartIndex, queryLabelEndIndex);
        int[] globalSizes = new int[] { dataNodeCount };


//        System.out.println("Query node: " + queryNode.getId());
//        System.out.println("Query node degree: " + queryNodeDegree);
//        System.out.println("Query node dataLabels: " + Arrays.toString(queryNodeLabels));

        CLEvent checkCandidatesEvent = this.checkCandidatesKernel.check_candidates(
                this.queue,
                this.queryNodeLabels,
                (int) queryNode.getId(),
                queryLabelStartIndex,
                queryLabelEndIndex,
                queryNodeDegree,
                this.dataLabelIndices,
                this.dataLabels,
                this.dataAdjacencyIndices,
                candidateIndicators,
                dataNodeCount,
                globalSizes,
                null);

        this.candidateIndicatorsPointer = candidateIndicators.read(this.queue, checkCandidatesEvent);
    }

    public int[] gatherCandidateArray(int nodeId) {
        int[] prefixScanArray = new int[dataNodeCount];
        int candidateCount = 0;
        int offset = dataNodeCount * nodeId;
        if(candidateIndicatorsPointer.get(offset + 0)) {
            candidateCount++;
        }

        for (int i = 1; i < dataNodeCount; i++) {
            int nextElement = candidateIndicatorsPointer.get(offset + i-1) ? 1 : 0;
            prefixScanArray[i] = prefixScanArray[i-1] + nextElement;
            if(candidateIndicatorsPointer.get(offset + i)) {
                candidateCount++;
            }
        }

        int[] candidateArray = new int[candidateCount];

        for (int i = 0; i < dataNodeCount; i++) {
            if(candidateIndicatorsPointer.get(offset + i)) {
                candidateArray[prefixScanArray[i]] = i;
            }
        }
        return candidateArray;
    }


    private void refineCandidates(int queryNodeId, int[] candidateArray) throws IOException {
        int queryNodeAdjacencyIndexStart = gpuQuery.getAdjacencyIndicies()[queryNodeId];
        int queryNodeAdjacencyIndexEnd = gpuQuery.getAdjacencyIndicies()[queryNodeId + 1];

        IntBuffer candidatesArrayBuffer = IntBuffer.wrap(candidateArray);
        CLBuffer<Integer> candidatesArray
                = this.context.createIntBuffer(CLMem.Usage.Input, candidatesArrayBuffer, true);
        int[] globalSizes = new int[] { candidateArray.length };

        CLEvent refineCandidatesEvent = this.refineCandidatesKernel.refine_candidates(
                queue,
                queryNodeId,
                queryNodeAdjacencies,
                queryNodeAdjacencyIndices,
                queryNodeLabels,
                queryNodeLabelIndices,
                queryNodeAdjacencyIndexStart,
                queryNodeAdjacencyIndexEnd,
                dataAdjacences,
                dataAdjacencyIndices,
                dataLabels,
                dataLabelIndices,
                candidatesArray,
                candidateIndicators,
                this.dataNodeCount,
                globalSizes,
                null
        );

        this.candidateIndicatorsPointer = candidateIndicators.read(this.queue, refineCandidatesEvent);
    }
}
