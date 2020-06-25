package com.mongodb.benchmark.drivers.threads;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.benchmark.database.dao.*;
import com.mongodb.benchmark.globals.GlobalVariables;
import com.mongodb.benchmark.utilities.core_utils.StackTrace;
import com.mongodb.client.ClientSession;
import com.mongodb.client.TransactionBody;
import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jeffrey Schmidt
 */
public class LoadThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(LoadThread.class.getName());

    private static int minMemberId;
    private static int maxMemberId;
    private static int minClaimId;
    private static int maxClaimId;
    private static int minProviderId;
    private static int maxProviderId;

    private final int threadNumber_;
//    private final int batchSize_;

    public LoadThread(int threadNumber,
                      int minMemberId, int maxMemberId,
                      int minClaimId, int maxClaimId,
                      int minProviderId, int maxProviderId) {
        threadNumber_ = threadNumber;
        this.minMemberId = minMemberId;
        this.maxMemberId = maxMemberId;
        this.minClaimId = minClaimId;
        this.maxClaimId = maxClaimId;
        this.minProviderId = minProviderId;
        this.maxProviderId = maxProviderId;
    }

    private int randomNum(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    @Override
    public void run() {

        System.out.println("Running Thread");

        final TransactionOptions txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        // will run forever till processes/server is killed
        while (true) {

            try {

                ClientSession sess = GlobalVariables.mongoClient_.startSession();

                // Set the id's
                int memberId = randomNum(minMemberId, maxMemberId);
                int claimId = randomNum(minClaimId, maxClaimId);
                int providerId = randomNum(minProviderId, maxProviderId);
                logger.debug("MemberId: " + memberId + ", ClaimId: " + claimId + ", providerId: " + providerId);

                long ts = System.currentTimeMillis();

                // Fetch docs outside of the transaction block and use secondary
                Document memberDoc = MemberDao.find(memberId);
                Document claimDoc = ClaimDao.find(claimId);
                Document providerDoc = ProviderDao.find(providerId);
                Document memberPolicyDoc = MemberPolicyDao.find(memberId);

                // Build out the transaction now
                TransactionBody txnBody = new TransactionBody<String>() {
                    public String execute() {
                        String oldVersion = "1"; // version of the doc in string form

                        /************************************************************************
                         * Member doc
                         * Update the last mod ts and version
                         * Update the existing doc with the new doc
                         * If the update isn't successful, audit this in the conflicts collection
                         *************************************************************************/
                        if (memberDoc != null) {
                            if (memberDoc.containsKey("ts")) {
                                memberDoc.replace("ts", ts);
                            } else {
                                memberDoc.append("ts", ts);
                            }
                            oldVersion = memberDoc.getString("version");
                            if (memberDoc.containsKey("version")) {
                                memberDoc.replace("version", (Integer.parseInt(oldVersion) + 1) + "");
                            } else {
                                memberDoc.append("version", (Integer.parseInt(oldVersion) + 1) + "");
                            }

                            try {
                                Document result = MemberDao.findOneAndReplace(sess, memberId, oldVersion, memberDoc);
                            } catch (Exception ex) {
                                logger.error("memberCol.findOneAndReplace error: " + ex.getMessage());
                            }
                        } else {
                            logger.warn("Member not found: id=" + memberId);
                        };

                        /************************************************************************
                         * Repeat now for claims
                         *************************************************************************/
                        if (claimDoc != null) {
                            if (claimDoc.containsKey("ts")) {
                                claimDoc.replace("ts", ts);
                            } else {
                                claimDoc.append("ts", ts);
                            }
                            oldVersion = claimDoc.getString("version");
                            if (claimDoc.containsKey("version")) {
                                claimDoc.replace("version", (Integer.parseInt(oldVersion) + 1) + "");
                            } else {
                                claimDoc.append("version", (Integer.parseInt(oldVersion) + 1) + "");
                            }

                            try {
                                Document result = ClaimDao.findOneAndReplace(sess, claimId, oldVersion, claimDoc);
                            } catch (Exception ex) {
                                logger.error("claimCol.findOneAndReplace error: " + ex.getMessage());
                            }

                        } else {
                            logger.warn("Claim not found: id=" + claimId);
                        }

                        /************************************************************************
                         * Repeat now for provider
                         *************************************************************************/
                        if (providerDoc != null) {
                            if (providerDoc.containsKey("ts")) {
                                providerDoc.replace("ts", ts);
                            } else {
                                providerDoc.append("ts", ts);
                            }
                            oldVersion = providerDoc.getString("version");
                            if (providerDoc.containsKey("version")) {
                                providerDoc.replace("version", (Integer.parseInt(oldVersion) + 1) + "");
                            } else {
                                providerDoc.append("version", (Integer.parseInt(oldVersion) + 1) + "");
                            }

                            try {
                                Document result = ProviderDao.findOneAndReplace(sess, providerId, oldVersion, providerDoc);
                            } catch (Exception ex) {
                                logger.error("providerCol.findOneAndReplace error: " + ex.getMessage());
                            }
                        } else {
                            logger.warn("Provider not found: id=" + providerId);
                        }

                        /************************************************************************
                         * Repeat now for member policy
                         *************************************************************************/
                        if (memberPolicyDoc != null) {
                            if (memberPolicyDoc.containsKey("ts")) {
                                memberPolicyDoc.replace("ts", ts);
                            } else {
                                memberPolicyDoc.append("ts", ts);
                            }
                            oldVersion = memberPolicyDoc.getString("version");
                            if (memberPolicyDoc.containsKey("version")) {
                                memberPolicyDoc.replace("version", (Integer.parseInt(oldVersion) + 1) + "");
                            } else {
                                memberPolicyDoc.append("version", (Integer.parseInt(oldVersion) + 1) + "");
                            }

                            try {
                                Document result = MemberPolicyDao.findOneAndReplace(sess, memberId, oldVersion, memberPolicyDoc);
                            } catch (Exception ex) {
                                logger.error("memberPolicyCol.findOneAndReplace error: " + ex.getMessage());
                            }
                        } else {
                            logger.warn("Member Policy not found: id=" + memberId);
                        }

                        return "Done";
                    };
                };

                sess.withTransaction(txnBody, txnOptions);

                sess.commitTransaction();
                sess.close();
            }
            catch (Exception e) {
                System.out.println(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
            }
            finally {
               // DatabaseUtils.cleanup(connection); // close the connection
            }

            GlobalVariables.completedSimulationIterations.incrementAndGet();
        }
    }
}
