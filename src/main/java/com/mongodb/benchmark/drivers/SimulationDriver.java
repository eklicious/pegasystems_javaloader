package com.mongodb.benchmark.drivers;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.Level;
import ch.qos.logback.core.util.StatusPrinter;
import com.mongodb.ConnectionString;
import com.mongodb.benchmark.database.dao.*;
import com.mongodb.benchmark.drivers.threads.LoadThread;
import com.mongodb.benchmark.utilities.core_utils.StackTrace;
import com.mongodb.benchmark.utilities.file_utils.FileIo;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.benchmark.drivers.threads.StatusCheckThread;
import com.mongodb.benchmark.globals.ApplicationConfiguration;
import com.mongodb.benchmark.globals.GlobalVariables;
import com.mongodb.benchmark.globals.MongoConfiguration;
import com.mongodb.benchmark.utilities.core_utils.Threads;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimulationDriver {
    
    private static final Logger logger = LoggerFactory.getLogger(SimulationDriver.class.getName());
    
    public static void main(String[] args) {

        boolean initializeSuccess = initializeApplication();
        if (!initializeSuccess) {
            logger.error("An error occurred during application initialization. Shutting down application...");
            System.exit(-1);
        }

        Thread statusCheckThread = new Thread(new StatusCheckThread(ApplicationConfiguration.getDelayBetweenStatusChecksMs()));
        statusCheckThread.start();
        
        startSimulationThreads();
    }

    /**
     * Load config files
     * @return
     */
    public static boolean initializeApplication() {

        // read app config settings
        boolean isApplicationConfigSuccess = ApplicationConfiguration.initialize(System.getProperty("user.dir") + File.separator + "conf" + File.separator + "application.ini");

        // initialize logger
        boolean isLogbackSuccess = readAndSetLogbackConfiguration(System.getProperty("user.dir") + File.separator + "conf", "logback-config.xml");
        
        // set mongo logging to warn to shut it up a bit
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.DEBUG);

        // connect to mongo (if enabled)
        boolean isMongoConnectSuccess = connectToMongo();
        boolean isMongoCollectionsSuccess = setMongoCollections();
        
        // if any initialization step did not complete successfully, return false
        if (!isApplicationConfigSuccess || !isLogbackSuccess
                || !isMongoConnectSuccess || !isMongoCollectionsSuccess) {
            logger.error("An error during application initialization. Exiting...");
            return false;
        }

        logger.info("Finish - Initialize application");
        
        return true;
    }

    private static boolean readAndSetLogbackConfiguration(String filePath, String fileName) {
        
        boolean doesConfigFileExist = FileIo.doesFileExist(filePath, fileName);
        
        if (doesConfigFileExist) {
            File logggerConfigFile = new File(filePath + File.separator + fileName);

            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

            try {
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(context);
                context.reset(); 
                configurator.doConfigure(logggerConfigFile);
                StatusPrinter.printInCaseOfErrorsOrWarnings(context);
                return true;
            } 
            catch (Exception e) {
                StatusPrinter.printInCaseOfErrorsOrWarnings(context);
                return false;
            }
        }
        else {
            return false;
        }
    }
    
    private static boolean connectToMongo() {

        // read mdb config settings
        boolean isDbConfigSuccess = MongoConfiguration.initialize(System.getProperty("user.dir") + File.separator + "conf" + File.separator + "database-mongo.ini");

        //connect to mongo
        boolean didMongoConnectSuccessfully = true;

        try {
            if ( isDbConfigSuccess &&
                    (MongoConfiguration.getMongoSrv() != null) && !MongoConfiguration.getMongoSrv().isBlank()) {
                ConnectionString connectionString = new ConnectionString(MongoConfiguration.getMongoSrv());
                MongoClientSettings mongoClientSettings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
                GlobalVariables.mongoClient_ = MongoClients.create(mongoClientSettings);

                GlobalVariables.mongoReadWriteDatabase_ = GlobalVariables.mongoClient_.getDatabase(MongoConfiguration.getMongoDb());

            }
        }
        catch (Exception e) {
            logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
            didMongoConnectSuccessfully = false;
        }

        return didMongoConnectSuccessfully;
    }

    public static List<Thread> startSimulationThreads() {

        List<Thread> simulateLoadThreads = new ArrayList<>();
        
        for (int i = 0; i < ApplicationConfiguration.getNumSimulationThreads(); i++) {
            LoadThread loadThread = new LoadThread(
                    i,
                    ApplicationConfiguration.getMinMemberId(),
                    ApplicationConfiguration.getMaxMemberId(),
                    ApplicationConfiguration.getMinClaimId(),
                    ApplicationConfiguration.getMaxClaimId(),
                    ApplicationConfiguration.getMinProviderId(),
                    ApplicationConfiguration.getMaxProviderId()
            );
            
            
            Thread simulateLoadThread = new Thread(loadThread);

            simulateLoadThreads.add(simulateLoadThread);
            simulateLoadThread.start();
            GlobalVariables.runningSimulationThreads.incrementAndGet();
            Threads.sleepMilliseconds(ApplicationConfiguration.getDelayBetweenSimulationThreadLaunchMs());
        }
        
        return simulateLoadThreads;
    }

    /**
     * Establish all the mongo collections
     * @return
     */
    private static boolean setMongoCollections() {
        boolean isMembersSetSuccess = MemberDao.setMongoCollection(GlobalVariables.mongoReadWriteDatabase_);
        boolean isClaimsSetSuccess = ClaimDao.setMongoCollection(GlobalVariables.mongoReadWriteDatabase_);
        boolean isProvidersSetSuccess = ProviderDao.setMongoCollection(GlobalVariables.mongoReadWriteDatabase_);
        boolean isMemberPoliciesSetSuccess = MemberPolicyDao.setMongoCollection(GlobalVariables.mongoReadWriteDatabase_);

        boolean areCollectionsAllSet = isMembersSetSuccess && isClaimsSetSuccess && isProvidersSetSuccess && isMemberPoliciesSetSuccess;
        return areCollectionsAllSet;
    }
    
}