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

    public static final String DB_PATH = "target/foo";
    public static final String DB_CONFIG_PATH = "target";
    public static final String EXPERIMENT_QUERY =
            "MATCH (a1),(b2),(a3),(c4)" +
            "WHERE (a3)<--(a1)-->(b2) AND (a3)<--(b2)-->(c4)<--(a3)" +
            "RETURN a1, b2, a3, c4";

    public static void main(String[] args) throws IOException {
        /* Setup database */
        DatabaseService databaseService = new DatabaseService(DB_PATH);
        ExperimentSetup experimentSetup = new ExperimentSetup();
        experimentSetup.fillDatabaseWithTestData(databaseService.getGraphDatabase());


        /* Cypher query run */
        CypherQueryRunner cypherQueryRunner = new CypherQueryRunner();
        cypherQueryRunner.runCypherQuery(databaseService, EXPERIMENT_QUERY);

        /* GPU query run */
        GpuQueryRunner gpuQueryRunner = new GpuQueryRunner();
        gpuQueryRunner.runGpuQuery(databaseService);

        /* Tear down database */
        databaseService.deleteData();
        databaseService.shutdown();
        System.out.println("foo");
    }

}
