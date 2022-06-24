package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.common.Global;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


public class SQLHandler {
    private final Manager manager;
    private final static String INSERT = "insert";
    private final static String DELETE = "delete";
    private final static String UPDATE = "update";
    private final static String BEGIN = "begin";
    private final static String COMMIT = "commit";
    private final static String SELECT = "select";
    private static String[] CMD_SET_WITHOUT_SELECT = {INSERT, DELETE, UPDATE, BEGIN, COMMIT};

    public SQLHandler(Manager manager) {
        this.manager = manager;
    }

    public ArrayList<QueryResult> evaluate(String statement, long session) {
        String stmt_head = statement.split("\\s+")[0];
        if (Arrays.asList(CMD_SET_WITHOUT_SELECT).contains(stmt_head.toLowerCase()) && session==0)
        {
            try(Database.DatabaseHandler db = manager.getCurrentDatabase()) {
                db.getDatabase().databaseLogger.writeLog(statement);
            }
            catch (Exception ignored) {

            }
        }
        System.out.println("session:" +session + "  " + statement);
        // "begin transaction"
        if (statement.equals(Global.LOG_BEGIN_TRANSACTION)) {
            ArrayList<QueryResult> queryResults = new ArrayList<QueryResult>();
            try{
                if (!manager.currentSessions.contains(session)){
                    manager.currentSessions.add(session);
//                    ArrayList<String> x_lock_tables = new ArrayList<>();
//                    manager.x_lockDict.put(session, x_lock_tables);
                } else{
                    System.out.println("session already in a transaction.");
                }
            } catch (Exception e){
                queryResults.add(new QueryResult(e.getMessage()));
                return queryResults;
            }
            queryResults.add(new QueryResult("start transaction."));
            return queryResults;
        }

        // Commit 语句
        if (statement.equals(Global.LOG_COMMIT)) {
            ArrayList<QueryResult> queryResults = new ArrayList<QueryResult>();
            try{
                if (manager.currentSessions.contains(session)){ // 在一个 transaction 当中
                    try(Database.DatabaseHandler db = manager.getCurrentDatabase()) {
                        String databaseName = db.getDatabase().getDatabaseName();
                        manager.currentSessions.remove(session);
                        // 释放这个 Session 拥有的所有写锁（？）这是个啥？Read Commited 隔离级别吗？
                        // 使用 Table Lock Manager 操作
//                    ArrayList<String> table_list = manager.x_lockDict.get(session);
//                    for (String table_name : table_list) {
//                        currentDB.getTableLockManager().releaseWriteLock(session, table_name);
//                    }
                        db.getDatabase().getTableLockManager().releaseSessionAllWriteLock(session);
//                    table_list.clear();
//                    manager.x_lockDict.put(session,table_list);
                        // 处理 Log 和文件，太长的话就抹掉
                        String databaseLogFilename = Database.getDatabaseLogFilePath(databaseName);
                        File file = new File(databaseLogFilename);
                        if (file.exists() && file.isFile() && file.length() > 50000) {
                            System.out.println("Clear database log");
                            manager.get(databaseName).getDatabase().databaseLogger.clearLog();
                            manager.persistDatabase(databaseName);
                        }
                    }
                } else {
                    System.out.println("session not in a transaction.");
                }
            }catch (Exception e){
                queryResults.add(new QueryResult(e.getMessage()));
                return queryResults;
            }
            queryResults.add(new QueryResult("commit transaction."));
            return queryResults;
        }
        SQLLexer lexer = new SQLLexer(CharStreams.fromString(statement));
        lexer.removeErrorListeners();
        lexer.addErrorListener(SQLErrorListener.instance);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);

        SQLParser parser = new SQLParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(SQLErrorListener.instance);

        try {
            ImpVisitor visitor = new ImpVisitor(manager, session);
            return (ArrayList<QueryResult>) visitor.visitParse(parser.parse());
        } catch (Exception e) {
            String message = "Exception: illegal SQL statement! Error message: " + e.getMessage();
            QueryResult result = new QueryResult(message);
            ArrayList<QueryResult> results = new ArrayList<>();
            results.add(result);
            return results;
        }
    }

}
