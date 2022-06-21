package cn.edu.thssdb.parser;


// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.common.Global;
import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.parser.item.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Cell;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;


import java.util.ArrayList;
import java.util.concurrent.locks.Condition;

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
     * Finished and Tested
     创建表格
     */
    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        try {
            if(manager.currentDatabase == null){
                throw new DatabaseNotExistException();
            }
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
                    maxLength = Integer.parseInt(columnDefItem.type_name().getChild(2).getText());
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
            for (int i = 0;i < columnList.size();i++){
                if(columnList.get(i).isPrimary()){
                    columnList.get(i).setNotNull(true);
                }
            }
            //获取Table constraints，将对应列设置为primary与notNull
            for (int i = 0;i<ctx.table_constraint().column_name().size();i++){
                String primary_column = ctx.table_constraint().column_name(i).getText();
                for (int j = 0;j<columnList.size();j++){
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
            manager.currentDatabase.create(tableName,columns);
        }catch(Exception e){
            return e.getMessage();
        }
        return "Create table " + ctx.table_name().getText() + ".";
    }

    /**
     * Finished and Tested
     表格项插入:  K_INSERT K_INTO table_name ( '(' column_name ( ',' column_name )* ')' )?
     K_VALUES value_entry ( ',' value_entry )* ;
     Insert_into 的特性同mysql一致，具体如下：
        *column_name的顺序可以不与表格的顺序一致；
        *不输入column_name时，value_entry的数量必须与列保持一致，空的列必须显式填充 null
        *column_name可以不包含所有列
     Attention:目前直接写入磁盘，见函数末 table.persist();
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        try{
            String tableName = ctx.table_name().getText();
            if(manager.currentDatabase == null){
                throw new DatabaseNotExistException();
            }
            Table table = this.manager.currentDatabase.get(tableName);
            //获取输入value_entry的字面量
            ArrayList<ArrayList<String>> valueEntryList_str_List = new ArrayList<>();
            for(SQLParser.Value_entryContext value_entry_ctx: ctx.value_entry()){
                ArrayList<String> strArray = new ArrayList<>();
                for(SQLParser.Literal_valueContext literal_value_ctx: value_entry_ctx.literal_value()){
                    String str = literal_value_ctx.getText();
                    strArray.add(str);
                }
                valueEntryList_str_List.add(strArray);
            }

            for(ArrayList<String> valueEntryList_str:valueEntryList_str_List){
                ArrayList<Cell> value_entry = new ArrayList<>();
                //分类讨论是否指定列
                if(ctx.column_name().size()==0){
                    //判断列数是否与table.columns一致会在Table.CheckValidRow中实现
                    //我们只需要根据字面量生成Cell，生成Row
                    for(int i = 0;i<valueEntryList_str.size();i++){
                        String columnType = table.columns.get(i).getColumnType().name().toUpperCase();
                        switch(columnType){
                            case "INT":{
                                int num = Integer.parseInt(valueEntryList_str.get(i));
                                Cell cell = new Cell(num);
                                value_entry.add(cell);
                                break;
                            }
                            case "LONG":{
                                long num = Long.parseLong(valueEntryList_str.get(i));
                                Cell cell = new Cell(num);
                                value_entry.add(cell);
                                break;
                            }
                            case "FLOAT":{
                                float f = Float.parseFloat(valueEntryList_str.get(i));
                                Cell cell = new Cell(f);
                                value_entry.add(cell);
                                break;
                            }
                            case "DOUBLE":{
                                double d = Double.parseDouble(valueEntryList_str.get(i));
                                Cell cell = new Cell(d);
                                value_entry.add(cell);
                                break;
                            }
                            case "STRING":{
                                Cell cell = new Cell(valueEntryList_str.get(i));
                                value_entry.add(cell);
                                break;
                            }
                            default: throw new ValueFormatInvalidException(". column type parser fault");
                        }
                    }
                }
                else{
                    //检查column name与value entry是否匹配
                    if(ctx.column_name().size()!=valueEntryList_str.size()){
                        int expectedLen = ctx.column_name().size();
                        int realLen = valueEntryList_str.size();
                        throw new SchemaLengthMismatchException(expectedLen,realLen," column name mismatch to value entry.");
                    }
                    //检查column name中是否有相同的列
                    for(int i = 0;i<ctx.column_name().size()-1;i++){
                        for(int j = i+1;j<ctx.column_name().size();j++){
                            if(ctx.column_name(i).getText().equals(ctx.column_name(j).getText())){
                                throw new DuplicateKeyException();
                            }
                        }
                    }
                    //初始化value_entry
                    for(int i = 0;i<table.columns.size();i++){
                        Cell cell = new Cell(Global.ENTRY_NULL);
                        value_entry.add(cell);
                    }
                    for(int i = 0;i < ctx.column_name().size();i++){
                        //找到与column_name对应的列所在的index和type
                        int index = -1;
                        String targetType = "null";
                        for(int j = 0;j<table.columns.size();j++){
                            if(table.columns.get(j).getColumnName().equalsIgnoreCase(ctx.column_name(i).getText())){
                                index = j;
                                targetType = table.columns.get(j).getColumnType().name().toUpperCase();
                                break;
                            }
                        }
                        if(index < 0){
                            throw new KeyNotExistException();
                        }
                        switch (targetType){
                            case "INT":{
                                int num = Integer.parseInt(valueEntryList_str.get(i));
                                Cell cell = new Cell(num);
                                value_entry.set(index,cell);
                                break;
                            }
                            case "LONG":{
                                long num = Long.parseLong(valueEntryList_str.get(i));
                                Cell cell = new Cell(num);
                                value_entry.set(index,cell);
                                break;
                            }
                            case "FLOAT":{
                                float f = Float.parseFloat(valueEntryList_str.get(i));
                                Cell cell = new Cell(f);
                                value_entry.set(index,cell);
                                break;
                            }
                            case "DOUBLE":{
                                double d = Double.parseDouble(valueEntryList_str.get(i));
                                Cell cell = new Cell(d);
                                value_entry.set(index,cell);
                                break;
                            }
                            case "STRING":{
                                Cell cell = new Cell(valueEntryList_str.get(i));
                                value_entry.set(index,cell);
                                break;
                            }
                            default: throw new ValueFormatInvalidException(". Target type is "+targetType);
                        }
                    }
                }
                //从value_entry生成row
                Row rowToInsert = new Row(value_entry);
                //调用table的接口来插入该行
                table.insert(rowToInsert);
                table.persist();
            }
        }catch(Exception e){
            return e.getMessage();
        }
        return "Insert into " + ctx.table_name().getText() + " successfully";
    }

    /**
     * TODO
     表格项删除
     */
    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        return "Delete from" + ctx.table_name().getText() + "successfully";
    }

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
     * TODO: bug -- return null?
     * TODO: 猜测的问题是可能没进到语句的处理没进到这个函数
     展示表 SHOW TABLE tableName
     */
    @Override
    public String visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx){
        try{
            if(manager.currentDatabase==null){
                throw new DatabaseNotExistException();
            }
            String tableName = ctx.table_name().getText();
            return manager.currentDatabase.get(tableName).toString();
        }
        catch(Exception e){
            return e.getMessage();
        }
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

    @Override
    public ComparerItem visitComparer(SQLParser.ComparerContext ctx){
        if(ctx.column_full_name()!=null){
            String tableName = ctx.column_full_name().table_name().getText();
            String columnName = ctx.column_full_name().column_name().getText();
            return new ComparerItem(ComparerType.COLUMN,tableName,columnName);
        }
        else if(ctx.literal_value()!=null){
            String literalValue = "null";
            if(ctx.literal_value().NUMERIC_LITERAL()!=null){
                literalValue = ctx.literal_value().NUMERIC_LITERAL().getText();
                return new ComparerItem(ComparerType.NUMBER,literalValue);
            }
            else if(ctx.literal_value().STRING_LITERAL()!=null){
                literalValue = ctx.literal_value().STRING_LITERAL().getText();
                return new ComparerItem(ComparerType.STRING,literalValue);
            }
            return new ComparerItem(ComparerType.NULL,literalValue);
        }
        //如果该项为null的话，return null
        return null;
    }
    @Override
    public ComparerItem visitExpression(SQLParser.ExpressionContext ctx){
        if(ctx.comparer()!=null){
            return (ComparerItem) visit(ctx.comparer());
        }
        else if (ctx.expression().size()==1){
            return (ComparerItem) visit(ctx.getChild(1));
        }
        else {
            ComparerItem compItem1 = (ComparerItem) visit(ctx.getChild(0));
            ComparerItem compItem2 = (ComparerItem) visit(ctx.getChild(2));

            if ((compItem1.type != ComparerType.NUMBER && compItem1.type!=ComparerType.COLUMN) ||
                    (compItem2.type != ComparerType.NUMBER && compItem2.type!=ComparerType.COLUMN)) {
                throw new TypeNotMatchException(compItem1.type, ComparerType.NUMBER);
            }

            return new ComparerItem(compItem1,compItem2,ctx.getChild(1).getText());
        }
    }

    @Override
    public ConditionItem visitCondition(SQLParser.ConditionContext ctx){
        ComparerItem comparerItem1 = (ComparerItem) visit(ctx.getChild(0));
        ComparerItem comparerItem2 = (ComparerItem) visit(ctx.getChild(2));
        return new ConditionItem(comparerItem1,comparerItem2,ctx.getChild(1).getText());
    }

    @Override
    public MultipleConditionItem visitMultiple_condition(SQLParser.Multiple_conditionContext ctx){
        if(ctx.getChildCount() == 1) {
            return new MultipleConditionItem((ConditionItem) visit(ctx.getChild(0)));
        }

        MultipleConditionItem m1 = (MultipleConditionItem) visit(ctx.getChild(0));
        MultipleConditionItem m2 = (MultipleConditionItem) visit(ctx.getChild(2));
        return new MultipleConditionItem(m1,m2,ctx.getChild(1).getText());
    }
}


