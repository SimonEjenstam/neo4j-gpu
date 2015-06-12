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
import java.util.HashSet;

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
    private CLBuffer<Integer> dataAdjacencyIndicies;
    private CLBuffer<Integer> dataLabels;
    private CLBuffer<Integer> dataLabelIndicies;
    private CLBuffer<Boolean> candidateIndicators;
    private CLBuffer<Integer> dataAdjacencies;
    private CLBuffer<Integer> queryNodeLabels;
    private CLBuffer<Integer> queryNodeLabelIndicies;
    private CLBuffer<Integer> queryNodeAdjacencies;
    private CLBuffer<Integer> queryNodeAdjacencyIndicies;
    private boolean[] initializedQueryNode;
    private Pointer<Boolean> candidateIndicatorsPointer;


    public GpuQuery(GpuGraphModel gpuData, GpuGraphModel gpuQuery, QueryGraph queryGraph) {
        this.gpuData = gpuData;
        this.gpuQuery = gpuQuery;
        this.queryGraph = queryGraph;
        this.context = JavaCL.createBestContext();
        this.queue = this.context.createDefaultQueue();
        this.dataNodeCount = gpuData.getNodeLabels().length;
        this.queryNodeCount = gpuQuery.getNodeLabels().length;
        this.initializedQueryNode = new boolean[queryNodeCount];
    }

    public void executeQuery(ArrayList<Node> visitOrder) throws IOException {
        createDataBuffers(dataNodeCount, queryNodeCount);

        candidateInitialization(visitOrder);

        System.out.println("--------------REFINEMENT STEP-------------");
        candidateRefinement(visitOrder);

        System.out.println("--------------FINDING CANDIDATE EDGES-------------");
        HashMap<Integer, EdgeCandidates> edgeCandidatesHashMap =  candidateEdgeSearch();

        System.out.println("--------------JOINING CANDIDATE EDGES-------------");
        candidateEdgeJoin(edgeCandidatesHashMap);
    }

    private void candidateEdgeJoin(HashMap<Integer, EdgeCandidates> edgeCandidatesHashMap) {
        ArrayList<Integer> visitedQueryEdges = new ArrayList<Integer>();
        ArrayList<Integer> visitedQueryVertices = new ArrayList<Integer>();

        CLBuffer<Integer> possiblePartialSolutions = initializePossiblePartialSolutions(edgeCandidatesHashMap, visitedQueryEdges, visitedQueryVertices);

    }

    private CLBuffer<Integer> initializePossiblePartialSolutions(HashMap<Integer, EdgeCandidates> edgeCandidatesHashMap, ArrayList<Integer> visitedQueryEdges, ArrayList<Integer> visitedQueryVertices) {
        int minCandidatesQueryEdgeId = findUnvisitedQueryEdgeId(edgeCandidatesHashMap, visitedQueryEdges);
        EdgeCandidates initialEdgeCandidates = edgeCandidatesHashMap.get(minCandidatesQueryEdgeId);

        int solutionSize = this.queryGraph.nodes.size();
        int initalPartialSolutionCount = initialEdgeCandidates.getCount();
        int[] possiblePartialSolutionsArray = fillCandidateEdges(initialEdgeCandidates);

        CLBuffer<Integer>
                possiblePartialSolutions = this.context.createIntBuffer(CLMem.Usage.Output, IntBuffer.wrap(possiblePartialSolutionsArray), true);

        for(int relationshipId : edgeCandidatesHashMap.keySet()) {
            if(!visitedQueryEdges.contains(relationshipId)) {
                EdgeCandidates edgeCandidates = edgeCandidatesHashMap.get(relationshipId);
                int startNodeId = edgeCandidates.getQueryStartNodeId();
                int endNodeId = edgeCandidates.getQueryEndNodeId();
                if(visitedQueryVertices.contains(startNodeId) && visitedQueryVertices.contains(endNodeId) ) {
                    /* Prune existing possible solutions */
                } else if(visitedQueryVertices.contains(startNodeId) || visitedQueryVertices.contains(endNodeId)) {
                    /* Combine candidate edges with existing possible solutions */
                }
            }
        }

        visitedQueryEdges.add(minCandidatesQueryEdgeId);
        visitedQueryVertices.add(initialEdgeCandidates.getQueryStartNodeId());
        visitedQueryVertices.add(initialEdgeCandidates.getQueryEndNodeId());

        return possiblePartialSolutions;
    }

    private int[] fillCandidateEdges(EdgeCandidates edgeCandidates) {
        int solutionNodeCount = this.queryGraph.nodes.size();
        int partialSolutionCount = edgeCandidates.getCount();
        int[] possiblePartialSolutionsArray = new int[solutionNodeCount*partialSolutionCount];
        Pointer<Integer> candidateStartNodes = edgeCandidates.getCandidateStartNodes().read(this.queue);
        Pointer<Integer> candidateEndNodes = edgeCandidates.getCandidateEndNodes().read(this.queue);
        Pointer<Integer> candidateEndNodeIndicies = edgeCandidates.getCandidateEndNodeIndicies().read(this.queue);
        int addedPartialSolutions = 0;
        int indexOffset = 0;
        while(addedPartialSolutions < edgeCandidates.getCount()) {


            for(int i = 0; i < edgeCandidates.getCandidateStartNodes().getElementCount(); i++) {

                int startNode = candidateStartNodes.get(i);
                int maxIndex;
                if(i + 1 < edgeCandidates.getCandidateStartNodes().getElementCount()) {
                    maxIndex  = candidateEndNodeIndicies.get(i+1);
                } else {
                    maxIndex = (int) edgeCandidates.getCandidateEndNodeIndicies().getElementCount();
                }

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

            CLBuffer<Integer> candidateEdgeCounts = countEdgeCandidates(queryStartNodeId, queryEndNodeId, candidatesArray);

            int totalCandidateEdgeCount = 0;
            int[] candidateEdgeEndNodeIndicies = new int[candidateArray.length];
            Pointer<Integer> candidateEdgeCountsPointer = candidateEdgeCounts.read(this.queue);
            for(int i = 0; i < candidateArray.length; i++) {
                candidateEdgeEndNodeIndicies[i] = totalCandidateEdgeCount;
                totalCandidateEdgeCount += candidateEdgeCountsPointer.get(i);
            }

            CLBuffer<Integer>
                    candidateEdgeEndNodeIndiciesBuffer = this.context.createIntBuffer(CLMem.Usage.Input, IntBuffer.wrap(candidateArray), true);

            CLBuffer<Integer> candidateEdgeEndNodes =
                    searchCandidateEdges(queryStartNodeId, queryEndNodeId, candidatesArray, candidateEdgeEndNodeIndiciesBuffer, totalCandidateEdgeCount);

            EdgeCandidates edgeCandidates = new EdgeCandidates(
                    queryStartNodeId,
                    queryEndNodeId,
                    candidateEdgeEndNodeIndiciesBuffer,
                    candidateEdgeEndNodes,
                    candidatesArray);

            edgeCandidatesHashMap.put((int) relationship.getId(), edgeCandidates);

        }

        return edgeCandidatesHashMap;
    }

    private CLBuffer<Integer> searchCandidateEdges(int queryStartNodeId, int queryEndNodeId, CLBuffer<Integer> candidatesArray,
                                                  CLBuffer<Integer> candidateEdgeEndNodeIndicies, int totalCandidateEdgeCount) throws IOException {

        CLBuffer candidateEdgeEndNodes = this.context.createIntBuffer(CLMem.Usage.Output, totalCandidateEdgeCount);


        int[] globalSizes = new int[] {(int) candidatesArray.getElementCount()};

        SearchEdgeCandidates kernel = new SearchEdgeCandidates(this.context);
        CLEvent searchEdgeCandidatesEvent = kernel.count_edge_candidates(
                this.queue,
                queryStartNodeId,
                queryEndNodeId,
                this.dataAdjacencies,
                this.dataAdjacencyIndicies,
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


    private CLBuffer<Integer> countEdgeCandidates(int queryStartNodeId, int queryEndNodeId, CLBuffer<Integer> candidateArray) throws IOException {

        CLBuffer candidateEdgeCounts = this.context.createIntBuffer(CLMem.Usage.Output, candidateArray.getElementCount());


        int[] globalSizes = new int[] {(int) candidateArray.getElementCount()};

        CountEdgeCandidates kernel = new CountEdgeCandidates(this.context);
        CLEvent countEdgeCandidatesEvent = kernel.count_edge_candidates(
                this.queue,
                queryStartNodeId,
                queryEndNodeId,
                this.dataAdjacencies,
                this.dataAdjacencyIndicies,
                candidateEdgeCounts,
                candidateArray,
                this.candidateIndicators,
                this.dataNodeCount,
                globalSizes,
                null
        );

        countEdgeCandidatesEvent.waitFor();
        return candidateEdgeCounts;
    }

    private void candidateRefinement(ArrayList<Node> visitOrder) throws IOException {
        boolean[] oldCandidateIndicators = pointerToArray(this.candidateIndicatorsPointer, this.dataNodeCount*this.queryNodeCount);
        boolean candidateIndicatorsHasChanged = true;
        while(candidateIndicatorsHasChanged) {
            System.out.println("Candidate indicators have been updated, refining again.");
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
                printCandidateIndicatorMatrix();
            }
            int[] candidateArray = gatherCandidateArray((int) queryNode.getId());
            System.out.println("Candidate array for query node " + queryNode.getId() + ": " + Arrays.toString(candidateArray));
            exploreCandidates((int) queryNode.getId(), candidateArray);
            printCandidateIndicatorMatrix();
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

        ExploreCandidates kernel = new ExploreCandidates(context);
        CLEvent exploreCandidatesEvent = kernel.explore_candidates(
            queue,
            queryNodeId,
            queryNodeAdjacencies,
            queryNodeAdjacencyIndicies,
            queryNodeLabels,
            queryNodeLabelIndicies,
            queryNodeAdjacencyIndexStart,
            queryNodeAdjacencyIndexEnd,
            dataAdjacencies,
            dataAdjacencyIndicies,
            dataLabels,
            dataLabelIndicies,
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
        this.queryNodeLabelIndicies = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeLabelIndiciesBuffer, true);
        this.queryNodeAdjacencies = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacenciesBuffer, true);
        this.queryNodeAdjacencyIndicies = this.context.createIntBuffer(CLMem.Usage.Input, queryNodeAdjacencyIndiciesBuffer, true);

        this.dataAdjacencies = this.context.createIntBuffer(CLMem.Usage.Input, dataAdjacenciesBuffer, true);
        this.dataAdjacencyIndicies = this.context.createIntBuffer(CLMem.Usage.Input, dataAdjacencyIndexBuffer, true);
        this.dataLabels = context.createIntBuffer(CLMem.Usage.Input, dataLabelsBuffer, true);
        this.dataLabelIndicies = context.createIntBuffer(CLMem.Usage.Input, dataLabelIndexBuffer, true);


        this.candidateIndicators = context.createBuffer(
                CLMem.Usage.Output,
                Pointer.pointerToBooleans(new boolean[dataNodeCount * queryNodeCount]));
    }

    public void checkCandidates(GpuGraphModel gpuQuery, Node queryNode) throws IOException {
        System.out.println("---------------------------");
        int queryLabelStartIndex = gpuQuery.getLabelIndicies()[((int) queryNode.getId())];
        int queryLabelEndIndex = gpuQuery.getLabelIndicies()[((int) queryNode.getId())+1];
        int queryNodeDegree = queryNode.getDegree();
        int[] queryNodeLabels = Arrays.copyOfRange(gpuQuery.getNodeLabels(), queryLabelStartIndex, queryLabelEndIndex);
        int[] globalSizes = new int[] { dataNodeCount };


        System.out.println("Query node: " + queryNode.getId());
        System.out.println("Query node degree: " + queryNodeDegree);
        System.out.println("Query node dataLabels: " + Arrays.toString(queryNodeLabels));

        CheckCandidates kernels = new CheckCandidates(context);
        CLEvent checkCandidatesEvent = kernels.check_candidates(
                this.queue,
                this.queryNodeLabels,
                (int) queryNode.getId(),
                queryLabelStartIndex,
                queryLabelEndIndex,
                queryNodeDegree,
                this.dataLabelIndicies,
                this.dataLabels,
                this.dataAdjacencyIndicies,
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

        RefineCandidates kernel = new RefineCandidates(context);
        CLEvent refineCandidatesEvent = kernel.refine_candidates(
                queue,
                queryNodeId,
                queryNodeAdjacencies,
                queryNodeAdjacencyIndicies,
                queryNodeLabels,
                queryNodeLabelIndicies,
                queryNodeAdjacencyIndexStart,
                queryNodeAdjacencyIndexEnd,
                dataAdjacencies,
                dataAdjacencyIndicies,
                dataLabels,
                dataLabelIndicies,
                candidatesArray,
                candidateIndicators,
                this.dataNodeCount,
                globalSizes,
                null
        );

        this.candidateIndicatorsPointer = candidateIndicators.read(this.queue, refineCandidatesEvent);
    }
}
