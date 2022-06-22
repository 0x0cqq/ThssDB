package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Cell;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Designed for the select query with join/filtering...
 * hasNext() looks up whether the select result contains a next row
 * next() returns a row, plz keep an iterator.
 */

public class QueryTable implements Iterator<Row> {
  public ArrayList<Table> tables;
  public boolean muti_table;
  public ArrayList<Column> columns = new ArrayList<>();
  public ArrayList<String> column_names = new ArrayList<>();
  public int i;
  Iterator<Row> iterator1;
  Iterator<Row> iterator2;
  int table_1_size;
  int table_2_size;
  int idx1;
  int idx2;
  Row cache;
  Iterable i1;
  Iterable i2;


  public QueryTable(ArrayList<Table> tables) {
    //! Edit by Mdh
    this.tables = tables;
    if (tables.size() == 1) {
      this.muti_table = false;
      table_1_size = tables.get(0).index.size();
      table_2_size = 1;
      iterator1 = tables.get(0).iterator();
    } else {
      // * 多表情况
      iterator1 = tables.get(0).iterator();
      iterator2 = tables.get(1).iterator();
      i1 = tables.get(0);
      i2 = tables.get(1);
      idx1 = tables.get(0).getPrimaryIndex();
      idx2 = tables.get(1).getPrimaryIndex();
      table_1_size = tables.get(0).index.size();
      table_2_size = tables.get(1).index.size();
      cache = iterator1.next();
    }
    for (Table t : tables) {
      this.columns.addAll(t.columns);
      for (Column c : t.columns) {
        this.column_names.add(c.getColumnName());
      }
    }
  }

  @Override
  public boolean hasNext() {
    // TODO 是否有下一个值，返回bool
    // * 因为要允许多表访问，要进行区别
    if (!muti_table) {
    // * 只有一个表
      return this.iterator1.hasNext();
    } else {
    // * 有两个表
      return iterator1.hasNext() || iterator2.hasNext();
    }
  }


  @Override
  public Row next() {
    // TODO 下一个返回值是什么，这里返回Row
    if (!muti_table) {
      // * 只有一个表
      return this.iterator1.next();
    } else {
      // * 有两个表
      if (!iterator2.hasNext()){
        cache = iterator1.next();
        iterator2 = i2.iterator();
      }
      return combineRow(cache, iterator2.next());
    }
  }

  public static Row combineRow(Row row1, Row row2) {
    ArrayList<Cell> res = new ArrayList<>();
    res.addAll(row1.getEntries());
    res.addAll(row2.getEntries());
    return new Row(res);
  }



}