package com.mongodb.benchmark.utilities.db_utills;

/**
 * @author Jeffrey Schmidt
 */
public interface DatabaseObject<T> {
    
    public boolean isEqual(T t);

    
}
