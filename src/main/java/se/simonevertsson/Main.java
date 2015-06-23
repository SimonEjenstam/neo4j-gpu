package se.simonevertsson;

import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.experiments.CypherQueryRunner;
import se.simonevertsson.experiments.ExperimentSetup;
import se.simonevertsson.experiments.GpuQueryRunner;

import java.io.IOException;

/**
 * Created by simon on 2015-05-12.
 */
public class Main {

    public static final String TEST_DB_PATH = "target/foo";
    public static final String DR_WHO_DB_PATH = "C:\\Users\\simon\\Documents\\Neo4j\\drwho";
    public static final String DR_WHO_DB_CONFIG_PATH = "C:\\Users\\simon\\Documents\\Neo4j";
//    public static final String EXPERIMENT_QUERY =
//            "MATCH (a1),(b2),(a3),(c4)" +
//            "WHERE (a3)<--(a1)-->(b2) AND (a3)<--(b2)-->(c4)<--(a3)" +
//            "RETURN a1, b2, a3, c4";


    public static final String EXPERIMENT_QUERY =
            "MATCH " +
                    "(a1)-->(b2)," +
                    "(a1)-->(a3)," +
                    "(b2)-->(a3)," +
                    "(b2)-->(c4)," +
                    "(a3)-->(c4)" +
                    "RETURN a1, b2, a3, c4";

//    public static final String EXPERIMENT_QUERY =
//            "MATCH " +
//                    "(a1)-->(a2)," +
//                    "(a2)-->(a3)," +
//                    "(a3)-->(a1)" +
//                    "RETURN a1, a2, a3";

    public static void main(String[] args) throws IOException {
        /* Setup database */
//        DatabaseService databaseService = new DatabaseService(TEST_DB_PATH);
//        ExperimentSetup experimentSetup = new ExperimentSetup();
//        experimentSetup.fillDatabaseWithTestData(databaseService.getGraphDatabase());

        DatabaseService databaseService = new DatabaseService(DR_WHO_DB_PATH, DR_WHO_DB_CONFIG_PATH);



        /* Cypher query run */
        CypherQueryRunner cypherQueryRunner = new CypherQueryRunner();
        String result = cypherQueryRunner.runCypherQuery(databaseService, EXPERIMENT_QUERY);
        System.out.println(result);

        /* GPU query run */
        GpuQueryRunner gpuQueryRunner = new GpuQueryRunner();
        gpuQueryRunner.runGpuQuery(databaseService);

        /* Tear down database */
        databaseService.deleteData();
        databaseService.shutdown();
        System.out.println("foo");
    }

}
