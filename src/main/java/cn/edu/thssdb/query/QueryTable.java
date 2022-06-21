package cn.edu.thssdb.query;

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
  private  Iterator<Row> iterator = tables[0];
  public QueryTable(ArrayList<Table> tables) {
    //! Edit by Mdh
    this.tables = tables;
  }

  @Override
  public boolean hasNext() {
    // TODO
    return iterator.hasNext();
  }


  @Override
  public Row next() {
    // TODO
    return iterator.next();
    }



}