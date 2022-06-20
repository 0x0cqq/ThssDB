package cn.edu.thssdb.parser.item;

import cn.edu.thssdb.exception.IndexExceedLimitException;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;

public class MultipleConditionItem {
    public String op;
    public MultipleConditionItem multiConditionItem1;
    public MultipleConditionItem multiConditionItem2;
    public ConditionItem conditionItem;
    public Boolean hasChild;

    public MultipleConditionItem(ConditionItem c){
        this.hasChild = false;
        this.conditionItem = c;
    }

    public MultipleConditionItem(MultipleConditionItem m1,MultipleConditionItem m2,String op){
        this.multiConditionItem1 = m1;
        this.multiConditionItem2 = m2;
        this.op = op;
    }

    public Boolean hasChild(){
        return this.hasChild;
    }

    public MultipleConditionItem getChild(int i){
        if (i == 0){
            return this.multiConditionItem1;
        }
        else if (i == 1){
            return this.multiConditionItem2;
        }
        else{
            throw new IndexExceedLimitException();
        }
    }

    /**
     * TODO: test
     *判断一行是否满足 multiConditions 条件
     *方法：递归地判断子结点是否满足
     */
    public Boolean evaluate(Row row, ArrayList<String> ColumnName){
        if(!hasChild){
            return conditionItem.evaluate(row,ColumnName);
        }
        else{
            Boolean leftCond = multiConditionItem1.evaluate(row,ColumnName);
            Boolean rightCond = multiConditionItem2.evaluate(row,ColumnName);
            if(op.equals("and")){return (leftCond && rightCond);}
            else{return (leftCond || rightCond);}
        }
    }
}
