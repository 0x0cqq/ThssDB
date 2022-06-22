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
            if (type == ComparerType.COLUMN) {
                Cell entry = row.getEntries().get(ColumnName.indexOf(this.columnName));
                if(entry == null){
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

    /** 将当前ComparerItem计算成类型为double的值
     *  可以主动调用此函数的ComparerItem:
        - hasChild == true
        - type为 NUMBER
        - type为 COLUMN,但对应entry的数据类型为NUMBER
     * 要求每一个Child的type只能为column或number(不支持string和null类型的+-/*)
     */
    public Double Calculate(Row row,ArrayList<String> ColumnName){
        if(!hasChild){
            Object value1 = getValue(row,ColumnName);
            if(value1 == null || value1 instanceof String){
                return null;
            }
            return Double.parseDouble(value1.toString());
        }
        else{
            Double value1 = this.comparerItem1.Calculate(row,ColumnName);
            Double value2 = this.comparerItem2.Calculate(row,ColumnName);
            Double value;
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
                    value = 0.0;
                }
            }
            String newLiteralValue;
            if(value.intValue() == value.doubleValue()){
                newLiteralValue=String.valueOf(value.intValue());
            }
            else{
                newLiteralValue = value.toString();
            }
            this.literalValue = newLiteralValue;
            return value;
        }
    }

    public boolean isNull(){
        return this.isNull;
    }
}
