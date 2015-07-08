package se.simonevertsson.query;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

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

    public void insertAlias(QueryNode node, String alias) {
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
}
