package se.simonevertsson.experiments;

import org.neo4j.graphdb.RelationshipType;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public enum RelationshipTypes implements RelationshipType
{
    KNOWS,
    COMPANION_OF,
    LOVES,
    ENEMY_OF,
    ALLY_OF
}

