package cn.edu.thssdb.parser.item;

import cn.edu.thssdb.exception.IndexExceedLimitException;
import cn.edu.thssdb.exception.InvalidComparatorException;
import cn.edu.thssdb.exception.TypeNotMatchException;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;

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
     * 判断某一行是否满足该条件
     *
     */
    public Boolean evaluate(Row row,ArrayList<String> columnName){
        try {
            int compareResult = 0;
            expr1.Calculate(row,columnName);
            Object value1 = expr1.getValue(row, columnName);
            //if(value1!=null){System.out.println("value1 = " + value1.getClass().toString() + " " + value1);}
            expr2.Calculate(row,columnName);
            Object value2 = expr2.getValue(row, columnName);
            //if(value2!=null){System.out.println("value2 = " + value2.getClass().toString() + " " + value2);}

            //先处理一方为null的情况
            if(value1 == null || value2 == null){
                if(comparator.equals("=")){
                    return value1 == value2;
                }
                else if(comparator.equals("<>")){
                    return value1 != value2;
                }
                else{
                    return false;
                }
            }

            //两者都不为null
            boolean isString1 = value1 instanceof String;
            boolean isString2 = value2 instanceof String;
            if ((isString1 && !isString2) || (!isString1 && isString2)) {
                throw new TypeNotMatchException(ComparerType.NUMBER, ComparerType.STRING);
            }

            //存在Double无法和Integer转换的问题，所以单独讨论Number的部分（坑死了）
            if(value1 instanceof Integer || value1 instanceof Double){
                Double newValue1 = Double.valueOf(value1.toString());
                Double newValue2 = Double.valueOf(value2.toString());
                compareResult = newValue1.compareTo(newValue2);
            }
            else{
                String newValue1 = value1.toString();
                String newValue2 = value2.toString();
                compareResult =newValue1.compareTo(newValue2);
            }

            boolean result=false;
            switch (comparator){
                case ">":  result = compareResult>0;break;
                case "<":  result = compareResult<0;break;
                case ">=": result = compareResult>=0;break;
                case "<=": result = compareResult<=0;break;
                case "=": result = compareResult==0;break;
                case "<>": result = compareResult!=0;break;
                default: throw new InvalidComparatorException(comparator);
            }
            //System.out.println(result);
            return result;
        }
        catch(Exception e){
            System.out.println("Get Error in ConditionItem.evaluate()\n"+e.getMessage());
            return null;
        }
    }

}
