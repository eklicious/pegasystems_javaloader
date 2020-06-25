package com.mongodb.benchmark.globals;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jeffrey Schmidt
 */
public class GlobalVariables {

    public final static AtomicLong completedSimulationIterations = new AtomicLong(0);
    public final static AtomicLong numOfInserts = new AtomicLong(0);
    public final static AtomicLong numOfUpdates = new AtomicLong(0);
    public final static AtomicLong numOfFinds = new AtomicLong(0);
    public final static AtomicLong runningSimulationThreads = new AtomicLong(0);
    
    public static MongoClient mongoClient_ = null;
    public static MongoDatabase mongoReadWriteDatabase_ = null;

}
