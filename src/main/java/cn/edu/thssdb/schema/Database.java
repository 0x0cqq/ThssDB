package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateTableException;
import cn.edu.thssdb.exception.FileIOException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.common.Global;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class Database {

  private String databaseName; // 数据库名称
  private HashMap<String, Table> tableMap;
  private LockManager tableLockManager;
  public Logger databaseLogger;
  private ReentrantReadWriteLock lock;

  public class DatabaseHandler implements AutoCloseable{
    private Database database;
    private Boolean haveReadLock;
    private Boolean haveWriteLock;
    public DatabaseHandler(Database database, Boolean read, Boolean write){
      this.database = database;
      this.haveReadLock = read;
      this.haveWriteLock = write;
      if(read){
        this.database.lock.readLock().lock();
      }
      if(write) {
        this.database.lock.writeLock().lock();
      }
    }

    public Database getDatabase() {
      return this.database;
    }
    // 当使用 try-with-resources 的时候
    @Override
    public void close() {
      if(haveWriteLock) {
        this.database.lock.writeLock().unlock();
        haveWriteLock = false;
      }
      if(haveReadLock) {
        this.database.lock.readLock().unlock();
        haveReadLock = false;
      }
    }
  }

  public DatabaseHandler getReadHandler() {
    return new DatabaseHandler(this, true, false);
  }
  public DatabaseHandler getWriteHandler() {
    return new DatabaseHandler(this, true, false);
  }

  public Database(String databaseName) {
    this.databaseName = databaseName;
    this.tableMap = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    this.tableLockManager = new LockManager(this);
    File tableFolder = new File(this.getDatabaseTableFolderPath());
    if(!tableFolder.exists())
      tableFolder.mkdirs();
    this.databaseLogger = new Logger(this.getDatabaseDirPath(),"log");
    recover();
  }


  // Operations: (basic) persist, create tables
  private void persist() {
    // 把各表的元数据写到磁盘上
    for (Table table : this.tableMap.values()) {
      String filename = table.getTableMetaPath();
      ArrayList<Column> columns = table.columns;
      try {
        FileOutputStream fileOutputStream = new FileOutputStream(filename);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
        for (Column column : columns)
          outputStreamWriter.write(column.toString() + "\n");
        outputStreamWriter.close();
        fileOutputStream.close();
      } catch (Exception e) {
        throw new FileIOException(filename);
      }
    }
    // 清除日志
    databaseLogger.clearLog();
  }

  public String getName() {
    return databaseName;
  }
  
  // 创建表。
  // 需要拥有 Database 写的权限
  public void create(String tableName, Column[] columns) {
    if (this.tableMap.containsKey(tableName))
      throw new DuplicateTableException(tableName);
    Table table = new Table(this.databaseName, tableName, columns);
    this.tableMap.put(tableName, table);
    this.persist();
  }

  // 根据 Table 的名称获取 Table 变量
  // 需要拥有读的锁。
  public Table.TableHandler get(String tableName) {
    if (!this.tableMap.containsKey(tableName))
      throw new TableNotExistException(tableName);
    return this.tableMap.get(tableName).getTableHandler();
  }

  // 根据 TableName 丢弃一张表
  public void drop(Long session, String tableName) {
    if (!this.tableMap.containsKey(tableName))
      throw new TableNotExistException(tableName);
    try(Table.TableHandler tb = this.get(tableName)) {
      Table table = tb.getTable();
      String filename = table.getTableMetaPath();
      File file = new File(filename);
      if (file.isFile() && !file.delete())
        throw new FileIOException(tableName + " _meta  when drop a table in database");
      tableLockManager.getWriteLock(session, tb);
      table.dropTable();
    }
    this.tableMap.remove(tableName);
  }

  public void tableInsert(Long session, Table.TableHandler tb, Row row){
    tableLockManager.getWriteLock(session, tb);
    tb.getTable().insert(row);
  }
  public void tableDelete(Long session, Table.TableHandler tb, Row row) {
    tableLockManager.getWriteLock(session, tb);
    tb.getTable().delete(row);
  }

  public void tableUpdate(Long session, Table.TableHandler tb, Cell primaryCell, Row row) {
    tableLockManager.getWriteLock(session, tb);
    tb.getTable().update(primaryCell, row);
  }

  // 丢弃整个数据库
  public void dropDatabase() {
    // 需要有数据库的写锁
    for (Table table : this.tableMap.values()) {
      File file = new File(table.getTableMetaPath());
      if (file.isFile()&&!file.delete())
        throw new FileIOException(this.databaseName + " _meta when drop the database");
      table.dropTable();
      }
    this.tableMap.clear();
    this.tableMap = null;
  }

  public void recover() {
    System.out.println("! try to recover database " + this.databaseName);
    File tableFolder = new File(this.getDatabaseTableFolderPath());
    File[] files = tableFolder.listFiles();
//        for(File f: files) System.out.println("...." + f.getName());
    if (files == null) return;

    // 找到 table 的 meta, 并且从文件中恢复数据库
    for (File file : files) {
      if (!file.isFile() || !file.getName().endsWith(Global.META_SUFFIX)) continue;
      try {
        String fileName = file.getName();
        String tableName = fileName.substring(0,fileName.length()-Global.META_SUFFIX.length());
        if (this.tableMap.containsKey(tableName))
          throw new DuplicateTableException(tableName);

        ArrayList<Column> columnList = new ArrayList<>();
        InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String readLine;
        while ((readLine = bufferedReader.readLine()) != null)
          columnList.add(Column.parseColumn(readLine));
        bufferedReader.close();
        reader.close();
        Table table = new Table(this.databaseName, tableName, columnList.toArray(new Column[0]));
        System.out.println(table.toString());
        for(Row row: table)
          System.out.println(row.toString());
        this.tableMap.put(tableName, table);
      } catch (Exception ignored) {
      }
    }
  }


  public void quit() {
    try {
      this.lock.readLock().lock();
      for (String tableName : this.tableMap.keySet()){
        try(Table.TableHandler tb = get(tableName)){
          tb.getTable().persist();
        }
      }
      this.persist();
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public LockManager getTableLockManager(){
    return this.tableLockManager;
  }

  // Find position
  public String getDatabaseDirPath(){
    return Global.DBMS_DIR + File.separator + "data" + File.separator + this.databaseName;
  }
  public String getDatabaseTableFolderPath(){
    return this.getDatabaseDirPath() + File.separator + "tables";
  }
  private String getDatabaseLogFilePath(){
    return this.getDatabaseDirPath() + File.separator + "log";
  }
  public static String getDatabaseLogFilePath(String databaseName){
    return Global.DBMS_DIR + File.separator + "data" + File.separator + databaseName + File.separator + "log";
  }

  // Other utils.
  public String getDatabaseName() { return this.databaseName; }
  public String getTableInfo(String tableName) { return get(tableName).toString(); }
  public String toString() {
      if (this.tableMap.isEmpty()) return "{\n[DatabaseName: " + databaseName + "]\n" + Global.DATABASE_EMPTY + "}\n";
      StringBuilder result = new StringBuilder("{\n[DatabaseName: " + databaseName + "]\n");
      for (String tableName : this.tableMap.keySet())
        try(Table.TableHandler tb = get(tableName)) {
          Table table = tb.getTable();
          if (table != null)
            result.append(table.toString());
        }
      return result.toString() + "}\n";
  }

  public String getTableInfo() {
    return this.toString();
  }
}
