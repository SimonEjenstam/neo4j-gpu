package se.simonevertsson.gpu.query.dictionary;

import java.util.*;

/**
 * Created by simon on 2015-07-10.
 */
public class QueryIdDictionary {

    private HashMap<Long, Integer> idToQueryId;

    private HashMap<Integer, Long> queryIdToIds;

    private int nextQueryId;

    public QueryIdDictionary() {
        this.idToQueryId = new HashMap<Long, Integer>();
        this.queryIdToIds = new HashMap<Integer, Long>();
        this.nextQueryId = 0;
    }

    public int add(long id) {
        if(this.idToQueryId.get(id) == null) {
            this.idToQueryId.put(id, this.nextQueryId);
            this.queryIdToIds.put(this.nextQueryId, id);
            this.nextQueryId++;
        }
        return this.idToQueryId.get(id);
    }

    public int getQueryId(long id) {
        return this.idToQueryId.get(id);
    }

    public long getId(int queryId) {
        return this.queryIdToIds.get(queryId);
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        Set<Long> ids = this.idToQueryId.keySet();
        List<Long> idList = new ArrayList<Long>(ids);
        Collections.sort(idList);

        for(long id : idList) {
            builder.append("Id " + id + ": " + this.idToQueryId.get(id));
            builder.append('\n');
        }

        return builder.toString();
    }
}
