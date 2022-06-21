package cn.edu.thssdb.parser.item;

import cn.edu.thssdb.exception.IndexExceedLimitException;
import cn.edu.thssdb.exception.TypeNotMatchException;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;
import java.util.concurrent.locks.Condition;

public class ConditionItem {
    public String comparator;//{> >= < <= == <>}
    public ComparerItem expr1;
    public ComparerItem expr2;

    public ConditionItem(ComparerItem expr1,ComparerItem expr2,String op){
        this.comparator = op;
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    public ComparerItem getChild(int i) {
        if(i == 0){
            return this.expr1;
        }
        else if(i == 1){
            return this.expr2;
        }
        else{
            throw new IndexExceedLimitException();
        }
    }

    /**
     * TODO: test
     * 判断某一行是否满足该条件
     *
     */
    public Boolean evaluate(Row row,ArrayList<String> columnName){
        if(expr1.hasChild || expr2.hasChild){
            double value1 = expr1.Calculate(row,columnName);
            double value2 = expr2.Calculate(row,columnName);
            return (value1 == value2);
        }
        else{
            Object value1 = expr1.getValue(row,columnName);
            Object value2 = expr2.getValue(row,columnName);

            if(!value1.getClass().toString().equals(value2.getClass().toString())){
                throw new TypeNotMatchException(ComparerType.NUMBER,ComparerType.STRING);
            }

            if(value1.getClass().toString().equalsIgnoreCase("string")){
                return (value1.equals(value2));
            }
            else if(value1.getClass().toString().equalsIgnoreCase("double")){
                return (value1 == value2);
            }
        }
        return null;
    }
}
