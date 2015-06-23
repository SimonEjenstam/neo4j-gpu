package se.simonevertsson.gpu;

import org.bridj.Pointer;

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
}