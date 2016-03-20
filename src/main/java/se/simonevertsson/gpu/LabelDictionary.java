package se.simonevertsson.gpu;

import java.util.Map;
import java.util.TreeMap;

/**
 * Dictionary class which contains Node labels and their corresponding generated GPU-friendly representation
 */
public class LabelDictionary {

  private Map<String, Integer> labelToIds;

  private Map<Integer, String> idToLabels;

  private int nextId;

  /**
   * Initializes a new label dictionary
   */
  public LabelDictionary() {
    labelToIds = new TreeMap<>();
    idToLabels = new TreeMap<>();
    nextId = 1;
  }

  /**
   * Inserts the supplied label to this dictionary, generating a new GPU representation if this label doesn't exist
   * in this dictionary yet.
   *
   * @param label The label which will get a generated translation and be inserted to this dictionary
   * @return
   */
  public int insertLabel(String label) {
    if (labelToIds.containsKey(label)) {
      return getIdForLabel(label);
    } else {
      int currentId = nextId;
      labelToIds.put(label, currentId);
      idToLabels.put(currentId, label);
      nextId++;
      return currentId;
    }
  }

  /**
   * Returns the GPU translation for the supplied label
   * @param label A node label whose generated translation should be returned
   * @return The GPU translation for the given label or  {@code -1} if it doesn't exist.
   */
  public int getIdForLabel(String label) {
    return labelToIds.getOrDefault(label, -1);
  }

  /**
   * Returns the Node label for the supplied GPU translation
   * @param labelId A GPU label previously translated from a Node label using {@link LabelDictionary#insertLabel(String)}
   * @return The label for the given GPU label id or {@code null} if it doesn't exist.
   */
  public String getLabelForId(int labelId) {
    return idToLabels.get(labelId);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, Integer> labelEntry : labelToIds.entrySet()) {
      builder.append(labelEntry.getKey() + ": " + labelToIds.get(labelEntry.getKey()) + "\n");
    }
    return builder.toString();
  }
}
