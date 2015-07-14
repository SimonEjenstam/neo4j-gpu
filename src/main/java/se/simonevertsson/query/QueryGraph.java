package se.simonevertsson.query;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import java.util.ArrayList;

/**
 * Created by simon on 2015-05-12.
 */
public class QueryGraph {

    public ArrayList<Node> nodes = new ArrayList<Node>();

    public ArrayList<Relationship> relationships = new ArrayList<Relationship>();

    public ArrayList<Relationship> spanningTree = new ArrayList<Relationship>();

    public ArrayList<Node> visitOrder = new ArrayList<Node>();

    public AliasDictionary aliasDictionary = new AliasDictionary();

    public String toCypherQueryString() {
        StringBuilder builder = new StringBuilder();
        builder.append(toCypherQueryStringPrefix());
        builder.append(toCypherQueryStringSuffix());
        return builder.toString();
    }

    public String toCypherQueryStringPrefix() {
        StringBuilder builder = new StringBuilder();
        builder.append("MATCH ");
        boolean firstRelationship = true;
        for( Relationship relationship : this.relationships) {
            String startNodeAlias = this.aliasDictionary.getAliasForId((int) relationship.getStartNode().getId());
            String endNodeAlias = this.aliasDictionary.getAliasForId((int) relationship.getEndNode().getId());
            if(!firstRelationship) {
                builder.append(", ");
            } else {
                firstRelationship = false;
            }
            builder.append("(");
            builder.append(startNodeAlias);
            builder.append(")-->");
            builder.append("(");
            builder.append(endNodeAlias);
            builder.append(")");
        }
        return builder.toString();
    }

    public String toCypherQueryStringSuffix() {
        StringBuilder builder = new StringBuilder();
        builder.append(" RETURN ");
        boolean firstAlias = true;
        for(String alias : this.aliasDictionary.getAllAliases()) {
            if(!firstAlias) {
                builder.append(", ");
            } else {
                firstAlias = false;
            }
            builder.append(alias);
        }
        builder.append(";");
        return builder.toString();
    }
}
