package se.simonevertsson.query;

import org.neo4j.graphdb.Label;

/**
 * Created by simon.evertsson on 2015-05-18.
 */
public class QueryLabel implements Label {
    private final String name;

    public QueryLabel(String name) {
        this.name = name;
    }

    public String name() {
        return this.name;
    }
}
