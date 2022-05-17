package com.catalogue.pricing.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import static com.catalogue.pricing.commons.constants.CollectionConstants.ZONE_PRICES;
import static com.catalogue.pricing.commons.constants.CollectionConstants.PRODUCT_INFO;
import static com.catalogue.pricing.commons.constants.CollectionConstants.PINCODE_DISTANCES;
import static com.catalogue.pricing.commons.constants.ColumnConstants.ID;
import static com.catalogue.pricing.commons.constants.ColumnConstants.LEAST_PRICE;
import static com.catalogue.pricing.commons.constants.DBConstants.DATABASE;
import static org.apache.spark.sql.functions.udf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;

import com.catalogue.pricing.commons.constants.CollectionConstants;
import com.catalogue.pricing.commons.constants.DBConstants;
import com.catalogue.pricing.commons.constants.SparkUtils;
import com.catalogue.pricing.entities.Zone;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig; 

@Service
public class PricingServiceImpl implements PricingService,Serializable{
	
	transient private static final Logger logger = LoggerFactory.getLogger(PricingServiceImpl.class);
	
	@Autowired
	transient JavaSparkContext sparkContext;
	
	@Autowired
	transient SparkConf sparkConf;
	
	@Autowired
	transient SparkSession sparkSession;
	
	@Async
	@Override
	public void computeMin(String productId, Zone zone, Integer destPincode) {
		
		if(productId==null || productId.length()==0 || zone == null || destPincode==null || destPincode<=0) {
			logger.error(String.format("Invalid parameters to compute min productId=%s,zone=%s,destPincode=%d", productId,zone,destPincode));
			return;
		}
			
		logger.info(String.format("Started computing min for product %s and pincode %d", productId,destPincode));
		
		/* Generate collection name */
 		String zoneStr = zone.name().toLowerCase();
 		StringBuilder collectionBuilder = new StringBuilder();
 		collectionBuilder.append(zoneStr);
 		collectionBuilder.append(ZONE_PRICES);
 		String collectionName = collectionBuilder.toString();
		
 		
 		ReadConfig readConfig = SparkUtils.getReadConfig(collectionName,sparkConf);
 		
 		
 		// Fetch pincode prices for this product from respective zone prices collection
 		Dataset<Row> pincodePrices = MongoSpark.load(sparkContext,readConfig).withPipeline(Collections.singletonList(
                Document.parse("{ $match : { product : ObjectId(\""+productId+"\")}}")
		        )).toDF();
 		
 		if(!pincodePrices.isEmpty()) {
 			
 			List<Row>rows = new ArrayList<Row>();
 			rows.add(RowFactory.create(destPincode));
        	
 			String destPincodeStr = "destPin";
 			StructType structType = new StructType();
        	structType = structType.add(destPincodeStr, DataTypes.IntegerType,false);
        	Dataset<Row>destPinDataset = sparkSession.createDataFrame(rows, structType).toDF();
        	pincodePrices = destPinDataset.join(pincodePrices);
        	
        	readConfig = SparkUtils.getReadConfig(PRODUCT_INFO,sparkConf);
        	Dataset<Row>productInfoDataset = MongoSpark.load(sparkContext,readConfig).withPipeline(Collections.singletonList(
                    Document.parse("{ $match : { _id : ObjectId(\""+productId+"\")}}")
    		        )).toDF().limit(1).select(functions.col(ID));
        	
        	
        	if(!productInfoDataset.isEmpty()) {
        		
        		sparkSession.udf().register("distanceFunction", registerDistanceFunction());
        		
        		try {
        			Row row = pincodePrices.
       					 filter(functions.col("leastPrice.buyingPrice").isNotNull()).
       					 filter(functions.col("leastPrice.quantity").$greater(0)).
       					 withColumn("finalPrice", functions.call_udf("distanceFunction", 
       							functions.col("leastPrice.pincodeNum"),
       							functions.col(destPincodeStr),
       							functions.col("leastPrice.buyingPrice").cast(DataTypes.DoubleType))).
       					 sort(functions.col("finalPrice")).
       					 limit(1).first();
       		
        			Long minPrice = row.getAs("finalPrice");
        			
        			StructType details = new StructType(new StructField[]{
            			    new StructField("price", DataTypes.LongType, false, Metadata.empty())
            			   });
            		String colName = "prices."+destPincode+"";
            		StructType recordType = new StructType();
            		recordType = recordType.add(colName, details, false);
            		
            		
            		List<Row>l = new ArrayList<>();
            		l.add(RowFactory.create(RowFactory.create(minPrice)));
            		Dataset<Row>df = sparkSession.createDataFrame(l, recordType).toDF();
            		productInfoDataset = productInfoDataset.join(df);
            		
            		productInfoDataset.show();
            		WriteConfig writeConfig = SparkUtils.getWriteConfig(PRODUCT_INFO, sparkContext);
            	    MongoSpark.save(productInfoDataset,writeConfig);
				} catch (Exception e) {
					e.printStackTrace();
				}
        	}
 		}
 		
		logger.info(String.format("Finished computing min for product %s and pincode %d", productId,destPincode));
	}
	
	
	public UserDefinedFunction registerDistanceFunction() {
		return  udf(new UDF3<Integer,Integer,Double, Long>() {

			@Override
			public Long call(Integer t1,Integer t2,Double buyingPrice) throws Exception {
				
    			final MongoClient mongoclient = MongoClients.create("mongodb://localhost");
    			MongoDatabase database = mongoclient.getDatabase("catalogue");
    			MongoCollection<Document>pincodeCollection = database.getCollection("pincodedistances");
    			
    			BasicDBObject dbObject = new BasicDBObject();
    			dbObject.append("sourcePincode", t1);
    			dbObject.append("destPincode", t2);

    			
    			Document document = pincodeCollection.find(dbObject).first();
    			double distance = document.getDouble("distance");
    			double margin = 0.02;
    			mongoclient.close();
    			return Math.round(buyingPrice+(margin*buyingPrice)+distance);
			}
		}, DataTypes.LongType).asNonNullable();
	}
	
	@Override
	public void computeMinForZone(Zone zone) {
		// TODO Auto-generated method stub
		
	}
}
