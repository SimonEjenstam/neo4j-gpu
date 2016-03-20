package se.simonevertsson.gpu.dictionary;

import java.util.HashMap;

/**
 * Created by simon on 2015-05-13.
 */
public class TypeDictionary {

    private HashMap<String, Integer> typeToIds;

    private HashMap<Integer, String> idToTypes;

    private int nextId;

    public TypeDictionary() {
        typeToIds = new HashMap<String, Integer>();
        idToTypes = new HashMap<Integer, String>();
        nextId = 1;
    }

    public int insertType(String type) {
        if(typeToIds.containsKey(type)) {
            return getIdForType(type);
        } else {
            int currentId = nextId;
            typeToIds.put(type, currentId);
            nextId++;
            return currentId;
        }
    }

    public int getIdForType(String type) {
        return typeToIds.get(type) != null ? typeToIds.get(type) : -1;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for(String type : typeToIds.keySet()) {
            builder.append(type + ": " + typeToIds.get(type) + "\n");
        }
        return builder.toString();
    }
}
