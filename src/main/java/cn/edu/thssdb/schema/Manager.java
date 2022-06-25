package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.FileIOException;
import cn.edu.thssdb.parser.SQLHandler;
import cn.edu.thssdb.common.Global;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock(); // 用来保护 Manager 的文件
  private String currentDatabaseName;
  public ArrayList<Long> currentSessions;
  public ArrayList<Long> waitSessions;
  public static SQLHandler sqlHandler;
//  public HashMap<Long, ArrayList<String>> x_lockDict;
  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    databases = new HashMap<>();
    currentDatabaseName = null;
    sqlHandler = new SQLHandler(this);
//    x_lockDict = new HashMap<>();
    currentSessions = new ArrayList<>();
    File managerFolder = new File(Global.DBMS_DIR + File.separator + "data");
    if(!managerFolder.exists())
      managerFolder.mkdirs();
    this.recover();
  }

  public void deleteDatabase(String databaseName) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(databaseName))
        throw new DatabaseNotExistException(databaseName);
      try(Database.DatabaseHandler db = get(databaseName, false, true)) {
        db.getDatabase().dropDatabase();
      }
      databases.remove(databaseName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void switchDatabase(String databaseName) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(databaseName))
        throw new DatabaseNotExistException(databaseName);
      currentDatabaseName = databaseName;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
  // utils:

  // quit current manager
  public void quit() {
    try {
      lock.writeLock().lock();
      for (String databaseName : databases.keySet()){
        try(Database.DatabaseHandler db = get(databaseName, false, true)){
          db.getDatabase().quit();
        }
      }
      persist();
      databases.clear();
      currentDatabaseName = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Database.DatabaseHandler getCurrentDatabase(Boolean read, Boolean write) {
    try {
      lock.readLock().lock();
      if (currentDatabaseName == null)
        throw new DatabaseNotExistException("current Database");
      return  get(currentDatabaseName,read,write);
    } finally {
      lock.readLock().unlock();
    }
  }


  public Database.DatabaseHandler get(String databaseName, Boolean read, Boolean write) {
    try {
      lock.readLock().lock();
      if (!databases.containsKey(databaseName))
        throw new DatabaseNotExistException(databaseName);
      if(read)
        return  databases.get(databaseName).getReadHandler();
      else // write
        return databases.get(databaseName).getWriteHandler();
    } finally {
      lock.readLock().unlock();
    }
  }

  public String getDatabaseInfo(){
    try{
      lock.readLock().lock();
      String DatabaseInfo="You have databases below:\n";
      for(Map.Entry<String, Database> entry : databases.entrySet()){
        DatabaseInfo = DatabaseInfo + entry.getKey() + "\n";
      }
      return DatabaseInfo;
    }
    finally{
      lock.readLock().unlock();
    }
  }
  public void createDatabaseIfNotExists(String databaseName) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(databaseName))
        databases.put(databaseName, new Database(databaseName));
      if (currentDatabaseName == null) {
        if (!databases.containsKey(databaseName))
          throw new DatabaseNotExistException(databaseName);
        currentDatabaseName = databaseName;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void persist() {
    try {
      FileOutputStream fos = new FileOutputStream(Manager.getManagerDataFilePath());
      OutputStreamWriter writer = new OutputStreamWriter(fos);
      for (String databaseName : databases.keySet())
        writer.write(databaseName + "\n");
      writer.close();
      fos.close();
    } catch (Exception e) {
      throw new FileIOException(Manager.getManagerDataFilePath());
    }
  }

  public void persistDatabase(String databaseName) {
    try {
      lock.readLock().lock();
      try(Database.DatabaseHandler db = get(databaseName,true, false)){
        db.getDatabase().quit();
      }
      persist();
    } finally {
      lock.readLock().unlock();
    }
  }

  public void recover() {
    File managerDataFile = new File(Manager.getManagerDataFilePath());
    if (!managerDataFile.isFile()) return;
    try {
      System.out.println("??!! try to recover manager");
      InputStreamReader reader = new InputStreamReader(new FileInputStream(managerDataFile));
      BufferedReader bufferedReader = new BufferedReader(reader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        System.out.println("recover database name: " + line);
        createDatabaseIfNotExists(line);
        try (Database.DatabaseHandler db = get(line, false, true)) {
          Database database = db.getDatabase();
          // recover database
          database.recover();
          // use log to recover database(For those that is not on disk
          logRecover(database);
        }
      }
      bufferedReader.close();
      reader.close();
    } catch (Exception e) {
      throw new FileIOException(managerDataFile.getName());
    }
  }


  // use sql handler to re-execute those statements.
  public void logRecover(Database database) {
    this.currentDatabaseName = database.getDatabaseName();
    ArrayList<String> logs = database.databaseLogger.readLog();
    for(String statement : logs) {
      try {
        // use -1 to re-evaluate those statements
        sqlHandler.evaluate(statement, -1);
      } catch( Exception e ) {
        System.out.println("error when: " + statement);
      }
    }
    try{
      sqlHandler.evaluate("commit",-1);
    }catch(Exception ignored){

    }
  }


  // Get positions
  public static String getManagerDataFilePath(){
    return Global.DBMS_DIR + File.separator + "data" + File.separator + "manager";
  }

}
