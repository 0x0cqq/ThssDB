package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.FileIOException;
import cn.edu.thssdb.parser.SQLHandler;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.common.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// TODO: add lock control
// TODO: complete readLog() function according to writeLog() for recovering transaction

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  public Database currentDatabase;
  public ArrayList<Long> currentSessions;
  public ArrayList<Long> waitSessions;
  public static SQLHandler sqlHandler;
  public HashMap<Long, ArrayList<String>> x_lockDict;
  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO: init possible additional variables
    databases = new HashMap<>();
    currentDatabase = null;
    sqlHandler = new SQLHandler(this);
    x_lockDict = new HashMap<>();
    currentSessions = new ArrayList<>();
    File managerFolder = new File(Global.DBMS_DIR + File.separator + "data");
    if(!managerFolder.exists())
      managerFolder.mkdirs();
    this.recover();
  }

  public void deleteDatabase(String databaseName) {
    try {
      // TODO: add lock control
      if (!databases.containsKey(databaseName))
        throw new DatabaseNotExistException(databaseName);
      Database database = databases.get(databaseName);
      database.dropDatabase();
      databases.remove(databaseName);

    } finally {
      // TODO: add lock control
    }
  }

  public void switchDatabase(String databaseName) {
    try {
      // TODO: add lock control
      if (!databases.containsKey(databaseName))
        throw new DatabaseNotExistException(databaseName);
      currentDatabase = databases.get(databaseName);
    } finally {
      // TODO: add lock control
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }

  public Database getCurrentDatabase(){return currentDatabase;}

  // utils:

  // Lock example: quit current manager
  public void quit() {
    try {
      lock.writeLock().lock();
      for (Database database : databases.values())
        database.quit();
      persist();
      databases.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Database get(String databaseName) {
    try {
      // TODO: add lock control
      if (!databases.containsKey(databaseName))
        throw new DatabaseNotExistException(databaseName);
      return databases.get(databaseName);
    } finally {
      // TODO: add lock control
    }
  }

  public void createDatabaseIfNotExists(String databaseName) {
    try {
      // TODO: add lock control
      if (!databases.containsKey(databaseName))
        databases.put(databaseName, new Database(databaseName));
      if (currentDatabase == null) {
        try {
          // TODO: add lock control
          if (!databases.containsKey(databaseName))
            throw new DatabaseNotExistException(databaseName);
          currentDatabase = databases.get(databaseName);
        } finally {
          // TODO: add lock control
        }
      }
    } finally {
      // TODO: add lock control
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
      // TODO: add lock control
      Database database = databases.get(databaseName);
      database.quit();
      persist();
    } finally {
      // TODO: add lock control
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
        Database database = this.databases.get(line);
        // recover database
        database.recover();
        // use log to recover database
        // , but I don't understand this
        logRecover(database);
      }
      bufferedReader.close();
      reader.close();
    } catch (Exception e) {
      throw new FileIOException(managerDataFile.getName());
    }
  }


  // use sql handler to re execute those statement.
  public void logRecover(Database database) {
    this.currentDatabase = database;
    ArrayList<String> logs = database.databaseLogger.readLog();
    for(String statement : logs) {
      try {
        // use -1 to re-evaluate those statements
        sqlHandler.evaluate(statement, -1);
      } catch( Exception e ) {
        System.out.println("error when: " + statement);
      }
    }
  }


  // Get positions
  public static String getManagerDataFilePath(){
    return Global.DBMS_DIR + File.separator + "data" + File.separator + "manager";
  }
}
