package se.simonevertsson.gpu.graph;

import org.neo4j.graphdb.*;
import se.simonevertsson.gpu.query.dictionary.LabelDictionary;
import se.simonevertsson.gpu.query.dictionary.QueryIdDictionary;
import se.simonevertsson.gpu.query.dictionary.TypeDictionary;

import java.util.ArrayList;
import java.util.List;

/**
 * This class converts graph data data types, such as Node and Relationship etc. to a more GPU-firendly representation
 * such as arrays and primitive types.
 */
public class GpuGraphConverter {

  private List<Node> nodes;
  private LabelDictionary labelDictionary;
  private TypeDictionary typeDictionary;
  private QueryIdDictionary nodeIdDictionary;

  private ArrayList<Integer> labelIndices;
  private ArrayList<Integer> nodeLabels;
  private ArrayList<Integer> relationshipIndices;
  private ArrayList<Integer> nodeRelationships;
  private ArrayList<Integer> relationshipTypes;
  private QueryIdDictionary relationshipIdDictionary;
  private int currentLabelIndex;
  private int currentRelationshipIndex;

  /**
   * Creates a new converter.
   *
   * @param nodes           A list of all nodes in the database which should be queried.
   * @param labelDictionary A dictionary where the node labels and their generated GPU-representation will be stored.
   * @param typeDictionary  A dictionary where the relationship types and their generated GPU-representation will be stored.
   */
  public GpuGraphConverter(List<Node> nodes, LabelDictionary labelDictionary, TypeDictionary typeDictionary) {
    this.nodes = nodes;
    this.labelDictionary = labelDictionary;
    this.typeDictionary = typeDictionary;
  }

  /**
   * Converts the supplied database (node list) to a more GPU friendly representation
   *
   * @return
   */
  public GpuGraph convert() {
    initializeLists();
    for (Node node : this.nodes) {
      nodeIdDictionary.add(node.getId());
    }

    currentLabelIndex = 0;
    currentRelationshipIndex = 0;
    for (Node startNode : this.nodes) {
      addLabels(startNode);
      convertAndAddRelationships(startNode);
    }

    appendLastIndexIfNotEmpty(labelIndices, currentLabelIndex);
    appendLastIndexIfNotEmpty(relationshipIndices, currentRelationshipIndex);


    //  If no node has labels the label array will be empty. Empty arrays are not supported by JavaCL hence a
    //  dummy element is added in this case.
    if (nodeLabels.isEmpty()) {
      nodeLabels.add(-1);
    }

    return new GpuGraph(labelIndices, nodeLabels, relationshipIndices, nodeRelationships, relationshipTypes, nodeIdDictionary, relationshipIdDictionary);
  }

  private void convertAndAddRelationships(Node sourceNode) {
    Iterable<Relationship> nodeRelationships = sourceNode.getRelationships(Direction.OUTGOING);
    int relationshipCount = convertAndAddRelationships(nodeRelationships);

    relationshipIndices.add(currentRelationshipIndex);
    currentRelationshipIndex += relationshipCount;
  }

  private void addLabels(Node sourceNode) {
    Iterable<Label> labels = sourceNode.getLabels();
    int labelCount = convertAndAddLabels(labels);

    labelIndices.add(currentLabelIndex);
    currentLabelIndex += labelCount;
  }

  private void appendLastIndexIfNotEmpty(ArrayList<Integer> indexList, int lastIndex) {
    if (!indexList.isEmpty()) {
      indexList.add(lastIndex);
    }
  }

  private int convertAndAddRelationships(Iterable<Relationship> nodeRelationships) {
    int relationshipCount = 0;
    for (Relationship nodeRelationship : nodeRelationships) {
      this.relationshipIdDictionary.add(nodeRelationship.getId());
      int endNodeQueryId = this.nodeIdDictionary.getQueryId(nodeRelationship.getEndNode().getId());
      this.nodeRelationships.add(endNodeQueryId);
      if (nodeRelationship.getType() != null) {
        int relationshipTypeId = this.typeDictionary.insertType(nodeRelationship.getType().name());
        this.relationshipTypes.add(relationshipTypeId);
      } else {
        this.relationshipTypes.add(-1);
      }

      relationshipCount++;
    }

    return relationshipCount;
  }

  private int convertAndAddLabels(Iterable<Label> labels) {
    int labelCount = 0;
    for (Label nodeLabel : labels) {
      int nodeLabelId = this.labelDictionary.insertLabel(nodeLabel.name());
      this.nodeLabels.add(nodeLabelId);
      labelCount++;
    }

    return labelCount;
  }

  private void initializeLists() {
    this.nodeIdDictionary = new QueryIdDictionary();
    this.relationshipIdDictionary = new QueryIdDictionary();
    this.labelIndices = new ArrayList<Integer>();
    this.nodeLabels = new ArrayList<Integer>();
    this.relationshipIndices = new ArrayList<Integer>();
    this.nodeRelationships = new ArrayList<Integer>();
    this.relationshipTypes = new ArrayList<Integer>();
  }
}
