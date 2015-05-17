package se.simonevertsson;

import java.util.HashMap;

/**
 * Created by simon on 2015-05-13.
 */
public class LabelDictionary {

    private HashMap<String, Integer> labelToIds;

    private HashMap<Integer, String> idToLabels;

    private int nextId;

    public LabelDictionary() {
        labelToIds = new HashMap<String, Integer>();
        idToLabels = new HashMap<Integer, String>();
        nextId = 1;
    }

    public int insertLabel(String label) {
        if(labelToIds.containsKey(label)) {
            return getIdForLabel(label);
        } else {
            int currentId = nextId;
            labelToIds.put(label, currentId);
            nextId++;
            return currentId;
        }
    }

    public String getLabelFromId(int id) {
        return idToLabels.get(id);
    }

    public int getIdForLabel(String label) {
        return labelToIds.get(label);
    }
}
