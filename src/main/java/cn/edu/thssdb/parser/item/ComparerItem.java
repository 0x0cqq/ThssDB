package cn.edu.thssdb.parser.item;

import cn.edu.thssdb.exception.IndexExceedLimitException;
import cn.edu.thssdb.exception.TypeNotMatchException;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Cell;

import java.security.KeyException;
import java.util.ArrayList;

public class ComparerItem {
    public String tableName;
    public String columnName;
    public String literalValue;
    public ComparerType type;
    public ComparerItem comparerItem1;
    public ComparerItem comparerItem2;
    public String op;
    public boolean isNull;
    public boolean hasChild;

    public ComparerItem(){
        this.type = ComparerType.NULL;
        this.literalValue = "null";
        this.isNull = true;
        this.hasChild = false;
    }


    public ComparerItem(ComparerType type,String tableName,String columnName) {
        if (type == ComparerType.COLUMN) {
            this.type = type;
            this.tableName = tableName;
            this.columnName = columnName;
            this.hasChild = false;
        }
        else{
            throw new TypeNotMatchException(type,ComparerType.COLUMN);
        }
    }

    public ComparerItem(ComparerType type,String literalValue){
        this.type = type;
        this.literalValue = literalValue;
        this.hasChild = false;
        if(type == ComparerType.NULL) {
            this.isNull = true;
        }
    }

    public ComparerItem(ComparerItem compItem1,ComparerItem compItem2,String op){
        this.comparerItem1 = compItem1;
        this.comparerItem2 = compItem2;
        this.op = op;
        this.hasChild = true;
    }

    public Object getValue(Row row,ArrayList<String> ColumnName){
        try {
            //System.out.println("enter getValue(row,ColumnNames)");
            if (type == ComparerType.COLUMN) {
                //System.out.println("this.columnName = " + columnName);
                Cell entry = row.getEntries().get(ColumnName.indexOf(this.columnName));
                if(entry == null){
                    //System.out.println("ColumnName not exist.");
                    throw new KeyException();
                }
                return entry.value;
            } else if (type == ComparerType.NUMBER) {
                if(literalValue.contains(".")){
                    return Double.parseDouble(literalValue);
                }
                else{
                    return Integer.parseInt(literalValue);
                }
            } else if (type == ComparerType.STRING) {
                return literalValue;
            }
            return null;
        }
        catch (Exception e){
            System.out.println("Get Error in ComparerItem.getValue(Row,ArrayList<String>): " + e.getMessage());
            return null;
        }
    }

    public Object getValue(){
        if(type == ComparerType.COLUMN){
            throw new TypeNotMatchException(ComparerType.COLUMN,ComparerType.NUMBER);
        }
        else if(type == ComparerType.NUMBER){
            if(literalValue.contains(".")){
                return Double.parseDouble(literalValue);
            }
            else{
                return Integer.parseInt(literalValue);
            }
        }
        else if(type == ComparerType.STRING){
            return literalValue;
        }
        return null;
    }



    /** 由于想到了更好的方式，这个函数已经废弃
     * 将当前ComparerItem计算成类型为double的值
     *  可以主动调用此函数的ComparerItem:
        - hasChild == true
        - type为 NUMBER
        - type为 COLUMN,但对应entry的数据类型为NUMBER
     * 要求每一个Child的type只能为column或number(不支持string和null类型的+-/*)
     */
    public Double Calculate(Row row,ArrayList<String> ColumnName){
        if(!hasChild){
            double value;
            if(this.type == ComparerType.COLUMN){
                Object value1 = getValue(row,ColumnName);
                if(value1.getClass().toString().equalsIgnoreCase("STRING")){
                    throw new TypeNotMatchException(ComparerType.NUMBER,ComparerType.COLUMN);
                }
                else{
                    value = (Double) value1;
                }
            }
            else if(this.type == ComparerType.NUMBER){
                value = Double.parseDouble(this.literalValue);
            }
            else{
                throw new TypeNotMatchException(ComparerType.NUMBER,this.type);
            }
            return value;
        }
        else{
            Double value1 = this.comparerItem1.Calculate(row,ColumnName);
            Double value2 = this.comparerItem2.Calculate(row,ColumnName);
            double value;
            switch(op){
                case "+": {
                    value = value1 + value2;
                    break;
                }
                case "-":{
                    value = value1 - value2;
                    break;
                }
                case "*":{
                    value = value1 * value2;
                    break;
                }
                case "/":{
                    value = value1 / value2;
                    break;
                }
                default:{
                    value = 0;
                }
            }
            return value;
        }
    }

    public boolean isNull(){
        return this.isNull;
    }
}
