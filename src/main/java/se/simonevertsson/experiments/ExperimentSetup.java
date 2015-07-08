package se.simonevertsson.experiments;

import org.neo4j.graphdb.*;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class ExperimentSetup {

    private static final String MATRIX_INSERT_QUERY =
            "create (Neo:Crew {name:'Neo'}), (Morpheus:Crew {name: 'Morpheus'}), (Trinity:Crew {name: 'Trinity'}), (Cypher:Crew:Matrix {name: 'Cypher'}), (Smith:Matrix {name: 'Agent Smith'}), (Architect:Matrix {name:'The Architect'}),\n" +
            "(Neo)-[:KNOWS]->(Morpheus), (Neo)-[:LOVES]->(Trinity), (Morpheus)-[:KNOWS]->(Trinity),\n" +
            "(Morpheus)-[:KNOWS]->(Cypher), (Cypher)-[:KNOWS]->(Smith), (Smith)-[:CODED_BY]->(Architect)";


    private static final String SMALL_DR_WHO_INSERT_QUERY =
            "create \n" +
                    "(Rose {character:'Rose Tyler'}), \n" +
                    "(Doctor {character:'Doctor'}), \n" +
                    "(Susan {character:'Susan Foreman'}), \n" +
                    "(Timelord {species:'Timelord'}), \n" +
                    "(Rose)-[:LOVES]->(Doctor), \n" +
                    "(Rose)-[:COMPANION_OF]->(Doctor), \n" +
                    "(Doctor)-[:LOVES]->(Rose),\n" +
                    "(Doctor)-[:IS_A]->(Timelord),\n" +
                    "(Susan)-[:COMPANION_OF]->(Doctor), \n" +
                    "(Susan)-[:IS_A]->(Timelord)";

    public void fillDatabaseWithTestData(GraphDatabaseService graphDatabase) {
        try
        {
            Transaction tx = graphDatabase.beginTx();


            Label labelA = DynamicLabel.label("A");
            Label labelB = DynamicLabel.label("B");
            Label labelC = DynamicLabel.label("C");

            Node A1 = graphDatabase.createNode();
            Node B2 = graphDatabase.createNode();
            Node A3 = graphDatabase.createNode();
            Node C4 = graphDatabase.createNode();

            A1.addLabel(labelA);
            B2.addLabel(labelB);
            A3.addLabel(labelA);
            C4.addLabel(labelC);

            A1.createRelationshipTo(B2, RelationshipTypes.KNOWS);
            A1.createRelationshipTo(A3, RelationshipTypes.KNOWS);
            B2.createRelationshipTo(A3, RelationshipTypes.KNOWS);
            B2.createRelationshipTo(C4, RelationshipTypes.KNOWS);
            A3.createRelationshipTo(C4, RelationshipTypes.KNOWS);

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void fillDatabaseWithMatrixTestData(GraphDatabaseService graphDatabase) {
        try
        {
            Transaction tx = graphDatabase.beginTx();

            graphDatabase.execute(MATRIX_INSERT_QUERY);

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void fillDatabaseWithSmallDrWhoTestData(GraphDatabaseService graphDatabase) {
        try
        {
            Transaction tx = graphDatabase.beginTx();

            graphDatabase.execute(SMALL_DR_WHO_INSERT_QUERY);

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
