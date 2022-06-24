package cn.edu.thssdb.query;


import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.MetaInfo;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.QueryResultType;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Designed to hold general query result:
 * In SQL result, the returned answer could be QueryTable OR an error message
 * For errors, resultType = QueryResultType.MESSAGE, see Construct method.
 * For results, it will hold QueryTable.
 */

public class QueryResult {

  public final QueryResultType resultType;
  public final String errorMessage; // If it is an error.

  private List<MetaInfo> metaInfoInfos;
  private List<String> columnNames;

  public List<Row> results;

  public QueryResult(QueryTable[] queryTables) {
    this.resultType = QueryResultType.SELECT;
    this.errorMessage = null;
    this.results = queryTables[0].results;
    this.columnNames = new ArrayList<>();
    for (Column column:queryTables[0].columns) {
      columnNames.add(column.getColumnName());
    }
  }

  public QueryResult(String errorMessage){
    resultType = QueryResultType.MESSAGE;
    this.errorMessage = errorMessage;
  }

  public static Row combineRow(LinkedList<Row> rows) {
    // TODO
    return null;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    return null;
  }

  public List<String> getColumnNames(){return this.columnNames;}
}
