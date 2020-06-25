package com.mongodb.benchmark.globals;

import com.mongodb.benchmark.utilities.config_utils.HierarchicalIniConfigurationWrapper;
import com.mongodb.benchmark.utilities.core_utils.StackTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jeffrey Schmidt
 */
public class MongoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MongoConfiguration.class.getName());

    public static final int VALUE_NOT_SET_CODE = -4444;

    private static boolean isInitializeSuccess_ = false;

    private static HierarchicalIniConfigurationWrapper mongoConfiguration_ = null;

    private static String mongoSrv_ = null;
    private static String mongoDb_ = null;

    public static boolean initialize(String filePathAndFilename) {
        
        if (filePathAndFilename == null) {
            return false;
        }
        
        mongoConfiguration_ = new HierarchicalIniConfigurationWrapper(filePathAndFilename);
        
        if (!mongoConfiguration_.isValid()) return false;     
        
        isInitializeSuccess_ = setMongoConfigurationValues();
        
        return isInitializeSuccess_;
    }
    
    private static boolean setMongoConfigurationValues() {

        try {
            mongoSrv_ = mongoConfiguration_.safeGetString("mongo_srv", "");
            mongoDb_ = mongoConfiguration_.safeGetString("mongo_database", "");
            return true;
        } 
        catch (Exception e) {
            logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
            return false;
        }
        
    }
    
    public static HierarchicalIniConfigurationWrapper getMongoConfiguration() {
        return mongoConfiguration_;
    }

    public static String getMongoSrv() {
        return mongoSrv_;
    }
    public static String getMongoDb() {
        return mongoDb_;
    }

}
