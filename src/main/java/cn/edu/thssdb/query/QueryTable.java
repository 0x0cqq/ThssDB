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
  public boolean muti_table;
  public Table table_1;
  public Table table_2;
  public int table_1_size = 999;
  public int i;
  public Iterator<Row> iterator;

  public QueryTable(ArrayList<Table> tables) {
    //! Edit by Mdh
    this.tables = tables;
    if (tables.size() == 1) {
      this.muti_table = false;
      this.table_1 = tables.get(0);
      this.iterator = table_1.iterator();
      table_2 = null;
      while (this.iterator.hasNext()) {
        table_1_size++;
      }
    } else {
      this.muti_table = true;
      this.table_1 = tables.get(0);
      this.table_2 = tables.get(1);
      this.iterator = table_1.iterator();
      this.i = 0;
      while (this.iterator.hasNext()) {
        table_1_size++;
      }
    }
  }

  @Override
  public boolean hasNext() {
    // TODO 是否有下一个值，返回bool
    // * 因为要允许多表访问，要进行区别
    if (!muti_table) {
    // * 只有一个表
      return this.iterator.hasNext();
    } else {
    // * 有两个表
      if (this.i <table_1_size -1 ) {
        this.i += 1;
        return this.iterator.hasNext();
      }else if (this.i == table_1_size-1) {
        this.iterator =table_2.iterator();
        return true;
      } else {
        return this.iterator.hasNext();
      }
    }
  }


  @Override
  public Row next() {
    // TODO 下一个返回值是什么，这里返回Row
    if (!muti_table) {
      // * 只有一个表
      return this.iterator.next();
    } else {
      // * 有两个表
      if (this.i <table_1_size -1 ) {
        this.i += 1;
        return this.iterator.next();
      }else if (this.i == table_1_size-1) {
        Row tmp = this.iterator.next();
        this.iterator =table_2.iterator();
        return tmp;
      } else {
        return this.iterator.next();
      }
    }
    }



}