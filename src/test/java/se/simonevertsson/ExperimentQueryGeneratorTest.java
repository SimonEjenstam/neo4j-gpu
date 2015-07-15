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

    public void testReturnsNullWhenRelationshipCountLessThan1orGreaterThan26() {
        // Arrange
        ExperimentQueryGraphGenerator negativeEQG = new ExperimentQueryGraphGenerator(this.allNodes, -1, 2);
        ExperimentQueryGraphGenerator greaterThan26EQG = new ExperimentQueryGraphGenerator(this.allNodes, -1, 2);

        // Act
        QueryGraph resultNegative = negativeEQG.generate();
        QueryGraph resultGreaterThan26 = greaterThan26EQG.generate();

        // Assert
        assertNull(resultNegative);
        assertNull(resultGreaterThan26);
    }

}
