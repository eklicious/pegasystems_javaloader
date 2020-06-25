package com.mongodb.benchmark.database.dao;

import com.mongodb.benchmark.globals.GlobalVariables;
import com.mongodb.benchmark.utilities.core_utils.StackTrace;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MemberPolicyDao {
    private static final Logger logger = LoggerFactory.getLogger(MemberPolicyDao.class.getName());
    private static MongoCollection<Document> localCollection = null;
    private static final String coll = "memberpolicies";

    public static boolean setMongoCollection(MongoDatabase mongoReadWriteDatabase) {
        try {
            localCollection = mongoReadWriteDatabase.getCollection(coll);
        }
        catch (Exception e) {
            logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
        }

        return true;
    }

    public static Document find(int id) {
        GlobalVariables.numOfFinds.incrementAndGet();
        return localCollection.find(com.mongodb.client.model.Filters.eq("data.MemberPolicy.MemberID", "M-" + id)).first();
    }

    public static Document findOneAndReplace(ClientSession sess, int id, String oldVersion, Document doc) {
        GlobalVariables.numOfUpdates.incrementAndGet();
        return localCollection.findOneAndReplace(sess,
                and(eq("data.MemberPolicy.MemberID", "M-" + id), eq("version", oldVersion)),
                doc);
    }

    public static boolean insert() {
        try {

//            localCollection.insertOne(testState.getState_BsonDocument(true));
            GlobalVariables.numOfInserts.incrementAndGet();

            return true;
        }
        catch (Exception e) {
            logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
            return false;
        }

    }
}
