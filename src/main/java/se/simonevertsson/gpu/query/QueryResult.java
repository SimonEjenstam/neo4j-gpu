package se.simonevertsson.gpu.query;

import java.util.List;

public class QueryResult {

  private final List<QuerySolution> querySolutions;
  private final long conversionExecutionTime;
  private final long queryExecutionTime;

  public QueryResult(List<QuerySolution> querySolutions, long conversionExecutionTime, long queryExecutionTime) {
    this.querySolutions = querySolutions;
    this.conversionExecutionTime = conversionExecutionTime;
    this.queryExecutionTime = queryExecutionTime;
  }


  public List<QuerySolution> getQuerySolutions() {
    return querySolutions;
  }

  public long getConversionExecutionTime() {
    return conversionExecutionTime;
  }

  public long getQueryExecutionTime() {
    return queryExecutionTime;
  }

  public long getTotalExecutionTime() {
    return conversionExecutionTime + queryExecutionTime;
  }
}
