package se.simonevertsson.gpu;

import com.nativelibs4java.opencl.CLBuffer;
import org.bridj.Pointer;
import org.neo4j.graphdb.Node;
import se.simonevertsson.Main;
import se.simonevertsson.query.AliasDictionary;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryUtils {

    public static int[] gatherCandidateArray(Pointer<Boolean> candidateIndicatorsPointer, int dataNodeCount, int nodeId) {
        int[] prefixScanArray = new int[dataNodeCount];
        int candidateCount = 0;
        int offset = dataNodeCount * nodeId;
        if (candidateIndicatorsPointer.get(offset + 0)) {
            candidateCount++;
        }

        for (int i = 1; i < dataNodeCount; i++) {
            int nextElement = candidateIndicatorsPointer.get(offset + i - 1) ? 1 : 0;
            prefixScanArray[i] = prefixScanArray[i - 1] + nextElement;
            if (candidateIndicatorsPointer.get(offset + i)) {
                candidateCount++;
            }
        }

        int[] candidateArray = new int[candidateCount];

        for (int i = 0; i < dataNodeCount; i++) {
            if (candidateIndicatorsPointer.get(offset + i)) {
                candidateArray[prefixScanArray[i]] = i;
            }
        }
        return candidateArray;
    }

    public static void printCandidateIndicatorMatrix(Pointer<Boolean> candidateIndicatorsPointer, int dataNodeCount) {
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

    public static void printPossibleSolutionsMatrix(Pointer<Integer> possibleSolutionPointer, int queryNodeCount) {
        StringBuilder builder = new StringBuilder();
        int j = 1;
        for(int solutionElement : possibleSolutionPointer)  {
            builder.append(solutionElement + ", ");
            if(j % queryNodeCount == 0) {
                builder.append("\n");
            }
            j++;
        }
        System.out.println(builder.toString());
    }

    public static boolean[] pointerBooleanToArray(Pointer<Boolean> pointer, int size) {
        boolean[] result = new boolean[size];
        int i = 0;
        for(boolean element : pointer) {
            result[i] = element;
            i++;
        }
        return result;
    }

    public static int[] pointerIntegerToArray(Pointer<Integer> pointer, int size) {
        int[] result = new int[size];
        int i = 0;
        for(int element : pointer) {
            result[i] = element;
            i++;
        }
        return result;
    }

    public static int[] generatePrefixScanArray(Pointer<Integer> bufferPointer, int bufferSize) {
        int totalElementCount = 0;
        int[] prefixScanArray = new int[bufferSize +1];
        for(int i = 0; i < bufferSize; i++) {
            prefixScanArray[i] = totalElementCount;
            totalElementCount += bufferPointer.get(i);
        }
        prefixScanArray[bufferSize] = totalElementCount;
        return prefixScanArray;
    }


    public static List<QuerySolution> generateQuerySolutions(QueryKernels queryKernels, QueryContext queryContext, CLBuffer<Integer> solutionsBuffer) {
        ArrayList<QuerySolution> results = new ArrayList<QuerySolution>();
        List<Map.Entry<String, Integer>> solutionElements = null;

        if(solutionsBuffer != null) {
            AliasDictionary aliasDictionary = queryContext.queryGraph.aliasDictionary;
            QueryIdDictionary queryGraphQueryIdDictionary = queryContext.gpuQuery.getQueryIdDictionary();
            QueryIdDictionary dataGraphQueryIdDictionary = queryContext.gpuData.getQueryIdDictionary();
            Pointer<Integer> solutionsPointer = solutionsBuffer.read(queryKernels.queue);
            int solutionCount = (int) (solutionsBuffer.getElementCount() / queryContext.queryNodeCount);


            for (int i = 0; i < solutionCount * queryContext.queryNodeCount; i++) {
                if (i % queryContext.queryNodeCount == 0) {
                    solutionElements = new ArrayList<Map.Entry<String, Integer>>();
                }
                int queryGraphQueryId = i % queryContext.queryNodeCount;
                int queryGraphId = (int) queryGraphQueryIdDictionary.getId(queryGraphQueryId);
                int solutionElementQueryId = solutionsPointer.get(i);
                int solutionElementId = (int) dataGraphQueryIdDictionary.getId(solutionElementQueryId);

                String alias = aliasDictionary.getAliasForId(queryGraphId);

                solutionElements.add(new AbstractMap.SimpleEntry<String, Integer>(alias, solutionElementId));
                if (i % queryContext.queryNodeCount == queryContext.queryNodeCount - 1) {
                    results.add(new QuerySolution(queryContext.queryGraph, solutionElements));
                }
            }
        }

        return results;
    }
}