package se.simonevertsson.experiments;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.query.QueryGraph;
import se.simonevertsson.query.QueryLabel;
import se.simonevertsson.query.QueryNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

/**
 * Created by simon.evertsson on 2015-09-02.
 */
public class RandomQueryGraphGenerator {
    private final HashSet<RelationshipType> relationshipTypes;
    private final HashSet<String> nodeLabels;
    private final int averageLabelCount;

    public RandomQueryGraphGenerator(DatabaseService databaseService) {
        this.relationshipTypes = new HashSet<>();
        this.nodeLabels = new HashSet<>();
        ArrayList<Node> allNodes = databaseService.getAllNodes();

        int totalLabelCount = 0;
        for(Node node : allNodes) {
            int labelCount = 0;
            for(Label label : node.getLabels()) {
                this.nodeLabels.add(label.name());
                labelCount++;
            }
            totalLabelCount = labelCount;
        }

        this.averageLabelCount = totalLabelCount/allNodes.size();

        for(Relationship relationship : databaseService.getAllRelationships()) {
            this.relationshipTypes.add(relationship.getType());
        }
    }

    public QueryGraph generate(int nodeCount, int averageDegree) {
        int currentNodeAlias = 0;
        QueryGraph queryGraph = new QueryGraph();

        Random random = new Random();


        /* Create nodes */
        for(int i = 0; i < nodeCount; i++) {
            QueryNode queryNode = new QueryNode(i);

            for(int j = 0; j < averageLabelCount; j++) {
                QueryLabel label = nextLabel(random);
                if(label != null) {
                    queryNode.addLabel(label);
                }
            }

            queryGraph.nodes.add(queryNode);
            queryGraph.aliasDictionary.insertAlias(queryNode, ExperimentUtils.createAlias(currentNodeAlias));
            currentNodeAlias++;
        }

        /* Create relationships */
        int relationshipId = 0;
        for(Node node : queryGraph.nodes) {
            for(int i = 0; i < averageDegree; i++) {
                /* Add a new relationship to a random node */
                int endNodeIndex = random.nextInt(queryGraph.nodes.size());
                Node endNode = queryGraph.nodes.get(endNodeIndex);

                Relationship relationship = ((QueryNode) node).createRelationshipTo(endNode, relationshipId, nextRelationshipType(random));
                queryGraph.relationships.add(relationship);
            }
        }


        return queryGraph;
    }

    private QueryLabel nextLabel(Random random) {
        int labelIndex = random.nextInt(this.nodeLabels.size());
        int index = 0;
        QueryLabel queryLabel = null;
        for(String label : this.nodeLabels)
        {
            if (index == labelIndex) {
               queryLabel = new QueryLabel(label);
                break;
            }
            index++;
        }
        return queryLabel;
    }

    private RelationshipType nextRelationshipType(Random random) {
        int relationshipTypeIndex = random.nextInt(this.relationshipTypes.size());
        int index = 0;
        RelationshipType relationshipType = null;
        for(RelationshipType type : this.relationshipTypes)
        {
            if (index == relationshipTypeIndex) {
                relationshipType = type;
                break;
            }
            index++;
        }
        return relationshipType;
    }
}
