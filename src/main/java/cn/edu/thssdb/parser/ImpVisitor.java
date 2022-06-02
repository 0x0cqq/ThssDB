package cn.edu.thssdb.parser;


// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.ColumnType;

import java.util.ArrayList;
import java.util.List;

/**
 * When use SQL sentence, e.g., "SELECT avg(A) FROM TableX;"
 * the parser will generate a grammar tree according to the rules defined in SQL.g4.
 * The corresponding terms, e.g., "select_stmt" is a root of the parser tree, given the rules
 * "select_stmt :
 *     K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
 *         K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;"
 *
 * This class "ImpVisit" is used to convert a tree rooted at e.g. "select_stmt"
 * into the collection of tuples inside the database.
 *
 * We give you a few examples to convert the tree, including create/drop/quit.
 * You need to finish the codes for parsing the other rooted trees marked TODO.
 */

public class ImpVisitor extends SQLBaseVisitor<Object> {
    private Manager manager;
    private long session;

    public ImpVisitor(Manager manager, long session) {
        super();
        this.manager = manager;
        this.session = session;
    }

    private Database GetCurrentDB() {
        Database currentDB = manager.getCurrentDatabase();
        if(currentDB == null) {
            throw new DatabaseNotExistException();
        }
        return currentDB;
    }

    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        if (ctx.create_db_stmt() != null) return new QueryResult(visitCreate_db_stmt(ctx.create_db_stmt()));
        if (ctx.drop_db_stmt() != null) return new QueryResult(visitDrop_db_stmt(ctx.drop_db_stmt()));
        if (ctx.use_db_stmt() != null)  return new QueryResult(visitUse_db_stmt(ctx.use_db_stmt()));
        if (ctx.create_table_stmt() != null) return new QueryResult(visitCreate_table_stmt(ctx.create_table_stmt()));
        if (ctx.drop_table_stmt() != null) return new QueryResult(visitDrop_table_stmt(ctx.drop_table_stmt()));
        if (ctx.insert_stmt() != null) return new QueryResult(visitInsert_stmt(ctx.insert_stmt()));
        if (ctx.delete_stmt() != null) return new QueryResult(visitDelete_stmt(ctx.delete_stmt()));
        if (ctx.update_stmt() != null) return new QueryResult(visitUpdate_stmt(ctx.update_stmt()));
        if (ctx.select_stmt() != null) return visitSelect_stmt(ctx.select_stmt());
        if (ctx.quit_stmt() != null) return new QueryResult(visitQuit_stmt(ctx.quit_stmt()));
        return null;
    }

    /**
     创建数据库
     */
    @Override
    public String visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        try {
            manager.createDatabaseIfNotExists(ctx.database_name().getText().toLowerCase());
            manager.persist();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Create database " + ctx.database_name().getText() + ".";
    }

    /**
     删除数据库
     */
    @Override
    public String visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        try {
            manager.deleteDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop database " + ctx.database_name().getText() + ".";
    }

    /**
     切换数据库
     */
    @Override
    public String visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        try {
            manager.switchDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Switch to database " + ctx.database_name().getText() + ".";
    }

    /**
     删除表格
     */
    @Override
    public String visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        try {
            GetCurrentDB().drop(ctx.table_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop table " + ctx.table_name().getText() + ".";
    }

    /**
     * Finished
     创建表格
     */
    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        try {
            String tableName = ctx.table_name().getText().toLowerCase();
            ArrayList<Column> columnList = new ArrayList<>();
            //获取columnItem，组成columnList
            for(int i = 0; i < ctx.column_def().size();i++){
                SQLParser.Column_defContext columnDefItem = ctx.column_def(i);
                String columnName = columnDefItem.column_name().getText().toLowerCase();

                String typeName = columnDefItem.type_name().getChild(0).getText().toUpperCase();
                ColumnType columntype = ColumnType.valueOf(typeName);
                //如果是String类型，获取maxLength
                int maxLength = 0;
                if(columnDefItem.type_name().getChildCount()>1){
                    maxLength = Integer.valueOf(columnDefItem.type_name().getChild(2).getText()).intValue();
                }
                boolean notNull = false;
                int primary = 0;
                for(int j = 0 ; j < columnDefItem.column_constraint().size();j++){
                    if(columnDefItem.column_constraint(j).getChild(1).getText().equalsIgnoreCase("NULL")){
                        notNull = true;
                    }
                    else if(columnDefItem.column_constraint(j).getChild(1).getText().equalsIgnoreCase("KEY")){
                        primary = 1;
                    }
                }
                //maxLength:限制每个值存储的长度，暂时设置为30，可能会修正
                Column column = new Column(columnName,columntype,primary,notNull,maxLength);
                columnList.add(column);
            }
            //更新主键对应column的notNull值
            for(int i = 0;i < columnList.size();i++){
                if(columnList.get(i).isPrimary()){
                    columnList.get(i).setNotNull(true);
                }
            }
            //获取Table constraints，将对应列设置为primary与notNull
            for(int i = 0;i<ctx.table_constraint().column_name().size();i++){
                String primary_column = ctx.table_constraint().column_name(i).getText();
                for(int j = 0;j<columnList.size();j++){
                    Column column = columnList.get(j);
                    if(primary_column.equalsIgnoreCase(column.getColumnName())){
                        column.setPrimary(1);
                        column.setNotNull(true);
                    }
                }
            }
            //从ArrayList传到数组里
            Column[] columns = columnList.toArray(new Column[0]);
            //建表
            this.manager.getCurrentDatabase().create(tableName,columns);
        }catch(Exception e){
            return e.getMessage();
        }
        return "Create table" + ctx.table_name() + ".";
    }

    /**
     * TODO
     表格项插入
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {return null;}

    /**
     * TODO
     表格项删除
     */
    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {return null;}

    /**
     * TODO
     表格项更新
     */
    @Override
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {return null;}

    /**
     * TODO
     表格项查询
     */
    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        return null;
    }

    /**
     * Finished
     展示表 SHOW TABLE tableName
     */
    @Override
    public String visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx){
        String str;
        try{
            String tableName = ctx.table_name().getText();
            Table table = manager.getCurrentDatabase().get(tableName);
            str = table.toString();
        }
        catch(Exception e){
            return e.getMessage();
        }
        return str;
    }
    /**
     退出
     */
    @Override
    public String visitQuit_stmt(SQLParser.Quit_stmtContext ctx) {
        try {
            manager.quit();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Quit.";
    }
    @Override
    public Object visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    @Override
    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<QueryResult> ret = new ArrayList<>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt()) ret.add(visitSql_stmt(subCtx));
        return ret;
    }
}


