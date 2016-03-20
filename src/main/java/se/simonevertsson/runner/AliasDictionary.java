package se.simonevertsson.runner;

import org.neo4j.graphdb.Node;

import java.util.*;

public class AliasDictionary {
  private HashMap<String, Integer> aliasToIds;

  private HashMap<Integer, String> idToAliases;

  public AliasDictionary() {
    aliasToIds = new HashMap<>();
    idToAliases = new HashMap<>();
  }

  public void insertAlias(Node node, String alias) {
    aliasToIds.put(alias, (int) node.getId());
    idToAliases.put((int) node.getId(), alias);
  }

  public int getIdForAlias(String alias) {
    return aliasToIds.get(alias);
  }

  public String getAliasForId(int id) {
    return idToAliases.get(id);
  }

  public Set<String> getAllAliases() {
    return aliasToIds.keySet();
  }

  public int getAliasCount() {
    return aliasToIds.size();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    Set<Integer> ids = this.idToAliases.keySet();
    List<Integer> idList = new ArrayList<Integer>(ids);
    Collections.sort(idList);

    for (int id : idList) {
      builder.append("Id " + id + ": " + this.idToAliases.get(id));
      builder.append('\n');
    }

    return builder.toString();
  }

  public static final int ALPHABET_SIZE = 26;

  public static final int ASCII_CHARACTER_START = 65;

  public static String createAlias(int currentNode) {
    StringBuilder builder = new StringBuilder();

    int aliasLength = (currentNode / ALPHABET_SIZE) + 1;
    int rest = currentNode;
    for (int i = 0; i < aliasLength; i++) {
      int aliasCharacter = rest % ALPHABET_SIZE;
      builder.append((char) (ASCII_CHARACTER_START + aliasCharacter));

      rest = rest - aliasCharacter - ALPHABET_SIZE;
    }

    return builder.reverse().toString();
  }


}
