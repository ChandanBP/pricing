package com.catalogue.pricing.commons.constants;

import static com.catalogue.pricing.commons.constants.SparkConstants.COLLECTION;
import static com.catalogue.pricing.commons.constants.SparkConstants.REPLACE_DOCUMENT;
import static com.catalogue.pricing.commons.constants.SparkConstants.SPARK_MONGO_OUTPUT_URI;
import static com.catalogue.pricing.commons.constants.SparkConstants.WRITE_CONCERN;
import static com.catalogue.pricing.commons.constants.CollectionConstants.PRODUCT_INFO;
import static com.catalogue.pricing.commons.constants.DBConstants.DATABASE;
import static com.catalogue.pricing.commons.constants.DBConstants.MONGODB;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.mongodb.BasicDBObject;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.sql.fieldTypes.api.java.ObjectId;

public class SparkUtils {
	
	public static ReadConfig getReadConfig(String collectionName,SparkConf sparkConf) {
		Map<String, String>configMap = new HashMap<>();
		configMap.put(COLLECTION, collectionName);
    	return ReadConfig.create(sparkConf).withOptions(configMap);
	}
	
	public static WriteConfig getWriteConfig(String collectionName,JavaSparkContext sparkContext) {
		
		Map<String, String> writeOverrides = new HashMap<String, String>();
		
		
		StringBuilder connectionBuilder = new StringBuilder();
		connectionBuilder.append(MONGODB);
		connectionBuilder.append(":");
		connectionBuilder.append("//");
		connectionBuilder.append("127.0.0.1");
		connectionBuilder.append("/");
		connectionBuilder.append(DATABASE);
		connectionBuilder.append(".");
		connectionBuilder.append(PRODUCT_INFO);
		
		System.out.println(connectionBuilder.toString());
		writeOverrides.put(SPARK_MONGO_OUTPUT_URI, connectionBuilder.toString());
		writeOverrides.put(COLLECTION, PRODUCT_INFO);
	    writeOverrides.put(WRITE_CONCERN, "majority");
	    writeOverrides.put(REPLACE_DOCUMENT, "false");
	    
	    return WriteConfig.create(sparkContext).withOptions(writeOverrides);
	}
	
	public static String matchProductId(String productId) {
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.append("product", new ObjectId(productId));
		return dbObject.toString();
	}
}
