package com.mongodb.benchmark.drivers.threads;

import com.mongodb.benchmark.globals.GlobalVariables;
import com.mongodb.benchmark.utilities.core_utils.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jeffrey Schmidt
 */
public class StatusCheckThread implements Runnable {
    
    private static final Logger logger = LoggerFactory.getLogger(StatusCheckThread.class.getName());
    
    private final int delayBetweenStatusChecksMs_;

    private long previousCompletedSimulationIterations_ = 0;
    private long previousCompletedInserts_ = 0;
    private long previousCompletedUpdates_ = 0;
    private long previousCompletedFinds_ = 0;

    public StatusCheckThread(int delayBetweenStatusChecksMs) {
        this.delayBetweenStatusChecksMs_ = delayBetweenStatusChecksMs;
    }
    
    @Override
    public void run() {
        
        while (true) {
            long currentCompletedSimulationIterations = GlobalVariables.completedSimulationIterations.get();
            long currentCompletedInserts = GlobalVariables.numOfInserts.get();
            long currentCompletedFinds = GlobalVariables.numOfFinds.get();
            long currentCompletedUpdates = GlobalVariables.numOfUpdates.get();

            
            long completedSimulationIterationsSinceLastStatusCheck = currentCompletedSimulationIterations - previousCompletedSimulationIterations_;
            float completedSimulationIterationsPerSecond = (completedSimulationIterationsSinceLastStatusCheck * 1000) / delayBetweenStatusChecksMs_;
            
            long completedCompletedInsertsSinceLastStatusCheck = currentCompletedInserts - previousCompletedInserts_;
            float completedCompletedInsertsPerSecond = (completedCompletedInsertsSinceLastStatusCheck * 1000) / delayBetweenStatusChecksMs_;

            long completedCompletedUpdatesSinceLastStatusCheck = currentCompletedUpdates - previousCompletedUpdates_;
            float completedCompletedUpdatesPerSecond = (completedCompletedUpdatesSinceLastStatusCheck * 1000) / delayBetweenStatusChecksMs_;

            long completedCompletedFindsSinceLastStatusCheck = currentCompletedFinds - previousCompletedFinds_;
            float completedCompletedFindsPerSecond = (completedCompletedFindsSinceLastStatusCheck * 1000) / delayBetweenStatusChecksMs_;

            logger.info("#ofThreads=" + GlobalVariables.runningSimulationThreads.get()
                    + ", " + "ItComplete=" + completedSimulationIterationsSinceLastStatusCheck 
                    + ", " + "It/s=" + completedSimulationIterationsPerSecond
                    + ", " + "Insert/s=" + completedCompletedInsertsPerSecond
                            + ", " + "Updates/s=" + completedCompletedUpdatesPerSecond
                            + ", " + "Finds/s=" + completedCompletedFindsPerSecond
                    );
            
            previousCompletedSimulationIterations_ = currentCompletedSimulationIterations;
            previousCompletedInserts_ = currentCompletedInserts;
            previousCompletedUpdates_ = currentCompletedUpdates;
            previousCompletedFinds_ = currentCompletedFinds;

            Threads.sleepMilliseconds(delayBetweenStatusChecksMs_);
        }
        
    }

}