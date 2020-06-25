package com.mongodb.benchmark.utilities.db_utills;

import java.sql.ResultSet;
import java.util.List;

/**
 * @author Jeffrey Schmidt
 */
public interface DatabaseResultSetHandler<T> {

    public <T> List<T> handleResultSet(ResultSet resultSet);
    
}
