package se.simonevertsson;

import junit.framework.TestCase;
import org.neo4j.graphdb.Node;
import se.simonevertsson.experiments.ExperimentQueryGraphGenerator;
import se.simonevertsson.query.QueryGraph;

import java.util.ArrayList;

/**
 * Created by simon.evertsson on 2015-07-14.
 */
public class ExperimentQueryGeneratorTest extends TestCase {

    ArrayList<Node> allNodes;


    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.allNodes = new ArrayList<>();
        this.allNodes = MockHelper.generateMockQueryGraph().nodes;
    }

    public void testReturnsAValidQueryGraph() {
        // Arrange
        ExperimentQueryGraphGenerator experimentQueryGraphGenerator = new ExperimentQueryGraphGenerator(this.allNodes, 4, 2);

        // Act
        QueryGraph result = experimentQueryGraphGenerator.generate();

        // Assert
        assertNotNull(result);
    }

}
