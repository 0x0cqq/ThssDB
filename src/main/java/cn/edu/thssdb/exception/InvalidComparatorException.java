package cn.edu.thssdb.exception;

public class InvalidComparatorException extends RuntimeException{
    public String key;

    public InvalidComparatorException(String key){
        this.key = key;
    }
    @Override
    public String getMessage(){
        return "Comparator " + key + " is invalid!";
    }

}
