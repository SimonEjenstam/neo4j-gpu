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

    public static boolean[] pointerToArray(Pointer<Boolean> pointer, int size) {
        boolean[] result = new boolean[size];
        int i = 0;
        for(boolean element : pointer) {
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
}