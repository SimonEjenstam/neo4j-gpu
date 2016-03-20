package se.simonevertsson;

import junit.framework.TestCase;
import org.neo4j.graphdb.Node;
import se.simonevertsson.experiments.QueryGraphGenerator;
import se.simonevertsson.query.QueryGraph;

import java.util.ArrayList;

/**
 * Created by simon.evertsson on 2015-07-14.
 */
public class QueryGraphGeneratorTest extends TestCase {

    ArrayList<Node> allNodes;


    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.allNodes = new ArrayList<>();
        this.allNodes = MockHelper.generateMockQueryGraph().nodes;
    }

    public void testReturnsAValidQueryGraph() {
        // Arrange
        QueryGraphGenerator queryGraphGenerator = new QueryGraphGenerator(this.allNodes, 4, 3);
        queryGraphGenerator.setMinimumQueryGraphAmount(1);

        // Act
        ArrayList<QueryGraph> result = queryGraphGenerator.generate(1);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
    }

}
