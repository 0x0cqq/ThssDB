package cn.edu.thssdb.exception;

public class IndexExceedLimitException extends RuntimeException{
    @Override
    public String getMessage(){
       return "Index Exceed Limit";
    }
}
