package se.simonevertsson.query;

import org.neo4j.graphdb.Node;

import java.util.*;

/**
 * Created by simon.evertsson on 2015-06-30.
 */
public class AliasDictionary {
    private HashMap<String, Integer> aliasToIds;

    private HashMap<Integer, String> idToAliases;

    public AliasDictionary() {
        aliasToIds = new HashMap<String, Integer>();
        idToAliases = new HashMap<Integer, String>();
    }

    public void insertAlias(Node node, String alias) {
        aliasToIds.put(alias, (int) node.getId());
        idToAliases.put((int) node.getId(), alias);
    }

    public int getIdForAlias(String alias) {
        return aliasToIds.get(alias);
    }

    public String getAliasForId(int id) { return idToAliases.get(id);}

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

        for(int id : idList) {
            builder.append("Id " + id + ": " + this.idToAliases.get(id));
            builder.append('\n');
        }

        return builder.toString();
    }

}
