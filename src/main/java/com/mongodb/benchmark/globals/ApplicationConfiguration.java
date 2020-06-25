package com.mongodb.benchmark.globals;

import com.mongodb.benchmark.utilities.config_utils.HierarchicalIniConfigurationWrapper;
import com.mongodb.benchmark.utilities.core_utils.StackTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jeffrey Schmidt
 */
public class ApplicationConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationConfiguration.class.getName());

    public static final int VALUE_NOT_SET_CODE = -4444;

    private static boolean isInitializeSuccess_ = false;

    private static HierarchicalIniConfigurationWrapper applicationConfiguration_ = null;

    private static int minMemberId_ = VALUE_NOT_SET_CODE;
    private static int maxMemberId_ = VALUE_NOT_SET_CODE;
    private static int minClaimId_ = VALUE_NOT_SET_CODE;
    private static int maxClaimId_ = VALUE_NOT_SET_CODE;
    private static int minProviderId_ = VALUE_NOT_SET_CODE;
    private static int maxProviderId_ = VALUE_NOT_SET_CODE;

    private static int numSimulationThreads_ = VALUE_NOT_SET_CODE;
    private static int delayBetweenSimulationThreadLaunchMs_ = VALUE_NOT_SET_CODE;
    private static int batchSize_ = VALUE_NOT_SET_CODE;
    private static int delayBetweenStatusChecksMs_ = VALUE_NOT_SET_CODE;
    
    private static boolean tryBulkInsert_ = false;

    public static boolean initialize(String filePathAndFilename) {
        
        if (filePathAndFilename == null) {
            return false;
        }
        
        applicationConfiguration_ = new HierarchicalIniConfigurationWrapper(filePathAndFilename);
        
        if (!applicationConfiguration_.isValid()) return false;     
        
        isInitializeSuccess_ = setApplicationConfigurationValues();
        
        return isInitializeSuccess_;
    }
    
    private static boolean setApplicationConfigurationValues() {

        try {            
            minMemberId_ = applicationConfiguration_.safeGetInt("min_member_id", 1);
            maxMemberId_ = applicationConfiguration_.safeGetInt("max_member_id", 1);
            minClaimId_ = applicationConfiguration_.safeGetInt("min_claim_id", 1);
            maxClaimId_ = applicationConfiguration_.safeGetInt("max_claim_id", 1);
            minProviderId_ = applicationConfiguration_.safeGetInt("min_provider_id", 1);
            maxProviderId_ = applicationConfiguration_.safeGetInt("max_provider_id", 1);

            numSimulationThreads_ = applicationConfiguration_.safeGetInt("num_simulation_threads", 5);
            delayBetweenSimulationThreadLaunchMs_ = applicationConfiguration_.safeGetInt("delay_between_simulation_thread_launch_ms", 10000);
            batchSize_ = applicationConfiguration_.safeGetInt("batch_size", 1);
            delayBetweenStatusChecksMs_ = applicationConfiguration_.safeGetInt("delay_between_status_checks_ms", 5000);
            
            tryBulkInsert_ = applicationConfiguration_.safeGetBoolean("try_bulk_insert", false); 

            return true;
        } 
        catch (Exception e) {
            logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
            return false;
        }
    }

    public static HierarchicalIniConfigurationWrapper getApplicationConfiguration() {
        return applicationConfiguration_;
    }

    public static boolean getTryBulkInsert() {
        return tryBulkInsert_;
    }

    public static int getMinMemberId() { return minMemberId_; }
    public static int getMaxMemberId() { return maxMemberId_; }
    public static int getMinClaimId() { return minClaimId_; }
    public static int getMaxClaimId() { return maxClaimId_; }
    public static int getMinProviderId() { return minProviderId_; }
    public static int getMaxProviderId() { return maxProviderId_; }

    public static int getNumSimulationThreads() {
        return numSimulationThreads_;
    }

    public static int getDelayBetweenSimulationThreadLaunchMs() {
        return delayBetweenSimulationThreadLaunchMs_;
    }
  
    public static int getBatchSize() {
        return batchSize_;
    }

    public static int getDelayBetweenStatusChecksMs() {
        return delayBetweenStatusChecksMs_;
    }
    
}
