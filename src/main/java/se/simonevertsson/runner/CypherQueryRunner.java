package se.simonevertsson.runner;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import se.simonevertsson.db.DatabaseService;
import se.simonevertsson.gpu.query.QueryResult;
import se.simonevertsson.gpu.query.QuerySolution;

import java.util.*;

/**
 * Class which executes Cypher queries on a supplied {@link DatabaseService} from a given {@link QueryGraph}.
 */
public class CypherQueryRunner {


  /**
   * Executes the Cypher runner generated from the supplied {@link QueryGraph} on the supplied {@link DatabaseService} and
   * returns the result of the runner as a {@link QueryResult}.
   *
   * @param dbService  A valid database service
   * @param queryGraph The runner graph which whose Cypher representation will be executed on the supplied database service.
   * @return
   */
  public QueryResult runCypherQueryForSolutions(DatabaseService dbService, QueryGraph queryGraph) {
    long tick = System.currentTimeMillis();

    List<QuerySolution> querySolutions;
    try (Transaction tx = dbService.beginTx()) {
      Result result = dbService.excuteCypherQuery(queryGraph.toCypherQueryString());
      querySolutions = handleResult(result, queryGraph);
      tx.success();
    }

    long tock = System.currentTimeMillis();
    long queryExecutionTime = tock - tick;

    return new QueryResult(querySolutions, 0, queryExecutionTime);
  }

  private List<QuerySolution> handleResult(Result result, QueryGraph queryGraph) {
    ArrayList<QuerySolution> results = new ArrayList<>();
    while (result.hasNext()) {
      List<Map.Entry<String, Integer>> solutionElements = new ArrayList<>();
      Map<String, Object> resultRow = result.next();
      for (String key : result.columns()) {
        Node resultNode = ((Node) resultRow.get(key));
        Map.Entry<String, Integer> solutionElement = new AbstractMap.SimpleEntry<>(key, (int) resultNode.getId());
        solutionElements.add(solutionElement);
      }

      QuerySolution querySolution = new QuerySolution(queryGraph, solutionElements);
      results.add(querySolution);
    }
    result.close();
    return results;
  }
}
