package se.simonevertsson.gpu;

import org.apache.lucene.util.ArrayUtil;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by simon on 2015-05-12.
 */
public class GpuGraphModel {

    private int[] labelIndicies;

    private int[] nodeLabels;

    private int[] adjacencyIndicies;

    private long[] nodeAdjecencies;

    public GpuGraphModel(ArrayList<Integer> labelIndicies, ArrayList<Integer> nodeLabels, ArrayList<Integer> adjecenyIndicies, ArrayList<Long> nodeAdjecencies) {
        this.labelIndicies = ArrayUtil.toIntArray(labelIndicies);
        this.nodeLabels = ArrayUtil.toIntArray(nodeLabels);
        this.adjacencyIndicies = ArrayUtil.toIntArray(adjecenyIndicies);
        this.nodeAdjecencies = convertArrayListToLongArray(nodeAdjecencies);
    }

    private long[] convertArrayListToLongArray(ArrayList<Long> longList) {
        long[] ret = new long[longList.size()];
        Iterator<Long> iterator = longList.iterator();
        for (int i = 0; i < ret.length; i++)
        {
            ret[i] = iterator.next().longValue();
        }
        return ret;
    }

    public int[] getNodeLabels() {
        return nodeLabels;
    }

    public int[] getAdjacencyIndicies() {
        return adjacencyIndicies;
    }

    public long[] getNodeAdjecencies() {
        return nodeAdjecencies;
    }

    public int[] getLabelIndicies() {
        return labelIndicies;
    }
}
