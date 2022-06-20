package cn.edu.thssdb.schema;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {
    public Database database;
    public ArrayList<ReentrantReadWriteLock.ReadLock> readLockList;
    public ArrayList<ReentrantReadWriteLock.WriteLock> writeLockList;

    public LockManager(Database database) {
        this.database = database;
        this.readLockList = new ArrayList<>();
        this.writeLockList = new ArrayList<>();
    }

    public void getReadLock(String tableName) {
        Table table = this.database.get(tableName);
        ReentrantReadWriteLock.ReadLock readLock = table.lock.readLock();
        readLock.lock();
        readLockList.add(readLock); // is that really ok ?? pretend that is ok though
        System.out.println(readLockList.toString());
    }

    public void releaseReadLock(String tableName) {
        Table table = this.database.get(tableName);
        ReentrantReadWriteLock.ReadLock readLock = table.lock.readLock();
        System.out.println(readLockList.toString());
        if(readLockList.remove(readLock)) {
            readLock.unlock();
        }
    }

    public void getWriteLock(String tableName) {
        Table table = this.database.get(tableName);
        ReentrantReadWriteLock.WriteLock writeLock = table.lock.writeLock();
        if(writeLock.isHeldByCurrentThread()){
            return;
        }
        writeLock.lock();
        writeLockList.add(writeLock);
        System.out.println(writeLockList.toString());
    }

    public void releaseWriteLock(String tableName) {
        Table table = this.database.get(tableName);
        ReentrantReadWriteLock.WriteLock writeLock = table.lock.writeLock();
        System.out.println(writeLockList.toString());
        if(writeLockList.remove(writeLock)) {
            writeLock.unlock();
        }
    }

}
