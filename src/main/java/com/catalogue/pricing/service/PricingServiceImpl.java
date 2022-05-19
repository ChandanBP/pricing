package com.catalogue.pricing.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import static com.catalogue.pricing.commons.constants.CollectionConstants.PINCODE;
import static com.catalogue.pricing.commons.constants.CollectionConstants.ZONE_PRICES;
import static com.catalogue.pricing.commons.constants.CollectionConstants.PRODUCT_INFO;
import static com.catalogue.pricing.commons.constants.ColumnConstants.ID;
import static org.apache.spark.sql.functions.udf;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import javax.annotation.PostConstruct;

import com.catalogue.pricing.commons.constants.ColumnConstants;
import com.catalogue.pricing.commons.constants.SparkUtils;
import com.catalogue.pricing.entities.MinPrice;
import com.catalogue.pricing.entities.Zone;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;

import scala.Tuple2;
import scala.reflect.internal.Trees.Return;

@Service
public class PricingServiceImpl implements PricingService, Serializable {

	transient private static final Logger logger = LoggerFactory.getLogger(PricingServiceImpl.class);

	@Autowired
	transient JavaSparkContext sparkContext;

	@Autowired
	transient SparkConf sparkConf;

	@Autowired
	transient SparkSession sparkSession;
	
	@Autowired
	transient MongoClient mongoClient;

	@Async
	@Override
	public void computeMin(String productId, Zone zone, Integer destPincode) {

		if (productId == null || productId.length() == 0 || zone == null || destPincode == null || destPincode <= 0) {
			logger.error(String.format("Invalid parameters to compute min productId=%s,zone=%s,destPincode=%d",
					productId, zone, destPincode));
			return;
		}

		logger.info(String.format("Started computing min for product %s and pincode %d", productId, destPincode));

		/* Generate collection name */
		String zoneStr = zone.name().toLowerCase();
		StringBuilder collectionBuilder = new StringBuilder();
		collectionBuilder.append(zoneStr);
		collectionBuilder.append(ZONE_PRICES);
		String collectionName = collectionBuilder.toString();

		ReadConfig readConfig = SparkUtils.getReadConfig(collectionName, sparkConf);

		// Fetch pincode prices for this product from respective zone prices collection
		Dataset<Row> pincodePrices = MongoSpark.load(sparkContext, readConfig)
				.withPipeline(Collections
						.singletonList(Document.parse("{ $match : { product : ObjectId(\"" + productId + "\")}}")))
				.toDF();

		if (!pincodePrices.isEmpty()) {

			List<Row> rows = new ArrayList<Row>();
			rows.add(RowFactory.create(destPincode));

			String destPincodeStr = "destPin";
			StructType structType = new StructType();
			structType = structType.add(destPincodeStr, DataTypes.IntegerType, false);
			Dataset<Row> destPinDataset = sparkSession.createDataFrame(rows, structType).toDF();
			pincodePrices = destPinDataset.join(pincodePrices);

			readConfig = SparkUtils.getReadConfig(PRODUCT_INFO, sparkConf);
			Dataset<Row> productInfoDataset = MongoSpark.load(sparkContext, readConfig)
					.withPipeline(Collections
							.singletonList(Document.parse("{ $match : { _id : ObjectId(\"" + productId + "\")}}")))
					.toDF().limit(1).select(functions.col(ID));

			if (!productInfoDataset.isEmpty()) {

				// sparkSession.udf().register("distanceFunction", registerDistanceFunction());

				try {
					Row row = pincodePrices.filter(functions.col("leastPrice.buyingPrice").isNotNull())
							.filter(functions.col("leastPrice.quantity").$greater(0))
							.withColumn("finalPrice",
									functions.call_udf("distanceFunction", functions.col("leastPrice.pincodeNum"),
											functions.col(destPincodeStr),
											functions.col("leastPrice.buyingPrice").cast(DataTypes.DoubleType)))
							.sort(functions.col("finalPrice")).limit(1).first();

					Long minPrice = row.getAs("finalPrice");

					StructType details = new StructType(new StructField[] {
							new StructField("price", DataTypes.LongType, false, Metadata.empty()) });
					String colName = "prices." + destPincode + "";
					StructType recordType = new StructType();
					recordType = recordType.add(colName, details, false);

					List<Row> l = new ArrayList<>();
					l.add(RowFactory.create(RowFactory.create(minPrice)));
					Dataset<Row> df = sparkSession.createDataFrame(l, recordType).toDF();
					productInfoDataset = productInfoDataset.join(df);

					WriteConfig writeConfig = SparkUtils.getWriteConfig(PRODUCT_INFO, sparkContext);
					MongoSpark.save(productInfoDataset, writeConfig);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		logger.info(String.format("Finished computing min for product %s and pincode %d", productId, destPincode));
	}

	@PostConstruct
	public void registerDistanceFunction() {
		UserDefinedFunction distanceFunction = udf(new UDF3<Integer, Integer, Double, Long>() {

			@Override
			public Long call(Integer t1, Integer t2, Double buyingPrice) throws Exception {

				// MongoClient mongoclient=null;
				try {

//					double distances[] = {35.77,17.09,138.15,166.93,68.4};
//					Random random = new Random();
//					
//					double distance = distances[random.nextInt(distances.length)]; 
//					double margin = 0.02;
					return Math.round(buyingPrice + (0.02 * buyingPrice) + 17.09);

//					mongoclient = MongoClients.create("mongodb://localhost");
//	    			MongoDatabase database = mongoclient.getDatabase("catalogue");
//	    			MongoCollection<Document>pincodeCollection = database.getCollection("pincodedistances");
//	    			
//	    			BasicDBObject dbObject = new BasicDBObject();
//	    			dbObject.append("sourcePincode", t1);
//	    			dbObject.append("destPincode", t2);
//
//	    			
//	    			Document document = pincodeCollection.find(dbObject).first();
//	    			
//	    			Number distance = document.get("distance", Number.class);
//	    			
//	    			double margin = 0.02;
//	    			
//	    			return Math.round(buyingPrice+(margin*buyingPrice)+distance.doubleValue());
				} catch (Exception e) {
					return Math.round(buyingPrice + (0.02 * buyingPrice));
				} finally {
//					if(mongoclient!=null)
//						mongoclient.close();
				}

			}
		}, DataTypes.LongType).asNonNullable();
		sparkSession.udf().register("distanceFunction", distanceFunction);
	}

	@Async
	@Override
	public void computeMinForZone(String productId,Zone zone) {
		
		if(productId==null || productId.length()==0 || zone == null) {
			logger.error(String.format("Invalid parameters to compute min productId=%s,zone=%s", productId,zone));
			return;
		}
			
		logger.info(String.format("Started computing min for product %s and zone %s", productId,zone.name()));
		
		ReadConfig readConfig = SparkUtils.getReadConfig(PINCODE,sparkConf);
		
		
		// Fetch all south pincodes
		Dataset<Row> pincodes = MongoSpark.load(sparkContext,readConfig).withPipeline(Collections.singletonList(
                Document.parse("{ $match : { zone : \""+zone.name()+"\"}}")
		        )).toDF().select(functions.col("pincode"));
		
		
		List<Row>destPincodes = pincodes.collectAsList();
		if(destPincodes==null||destPincodes.size()==0)return;
		
		/* Fetch pincode prices for this product from respective zone prices collection
		 Generate collection name */
		String zoneStr = zone.name().toLowerCase();
		StringBuilder collectionBuilder = new StringBuilder();
		collectionBuilder.append(zoneStr);
		collectionBuilder.append(ZONE_PRICES);
		String collectionName = collectionBuilder.toString();
		readConfig = SparkUtils.getReadConfig(collectionName,sparkConf);

		Dataset<Row> pincodePrices = MongoSpark.load(sparkContext,readConfig).withPipeline(Collections.singletonList(
		                Document.parse("{ $match : { product : ObjectId(\""+productId+"\")}}")
				        )).toDF().select(functions.col(ColumnConstants.LEAST_PRICE));
		
		// Fetch product info document
		readConfig = SparkUtils.getReadConfig(PRODUCT_INFO,sparkConf);
		Dataset<Row>productInfoDataset = MongoSpark.load(sparkContext,readConfig).withPipeline(Collections.singletonList(
                Document.parse("{ $match : { _id : ObjectId(\""+productId+"\")}}")
		        )).toDF().limit(1);
		
		HashMap<Integer, Long>map = new HashMap();
		List<Bson>bsons = new LinkedList<>();
		
		Long start = System.currentTimeMillis();
		
		try {
			StructType details = new StructType(new StructField[] {
		              new StructField("price", DataTypes.LongType, false, Metadata.empty()) });
			
			
			pincodes.javaRDD().cartesian(pincodePrices.javaRDD()).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Row,Row>>, Row, Row>() {

				@Override
				public Iterator<Tuple2<Row, Row>> call(Iterator<Tuple2<Row, Row>> t)
						throws Exception {
					
					MongoClient mongoclient = MongoClients.create("mongodb://localhost");
	    			MongoDatabase database = mongoclient.getDatabase("catalogue");
	    			MongoCollection<Document>pincodeCollection = database.getCollection("pincodedistances");
	    			
	    			List<Tuple2<Row,Row>> list =new LinkedList<>();
	    			while (t.hasNext()) {
						Tuple2<Row, Row>tuple2 = t.next();
						
						Row pinRow = tuple2._1;
						Integer srcPincode = pinRow.getAs("pincode");
						
						Row pricesRow = tuple2._2;
						Row pricesRow1 = (Row)pricesRow.getAs("leastPrice");
						Integer buyingPrice = pricesRow1.getAs("buyingPrice");
						Integer quantity = pricesRow1.getAs("quantity");
						Integer destPincode = pricesRow1.getAs("pincodeNum");
						
						
						
						if(buyingPrice!=null && quantity>0) {
			    			BasicDBObject dbObject = new BasicDBObject();
			    			dbObject.append("sourcePincode", srcPincode);
			    			dbObject.append("destPincode", destPincode);
			    			
			    			//System.out.println(srcPincode+","+destPincode);
			    			Number distance;
			    			
			    			if(srcPincode.intValue()==destPincode.intValue()) {
			    				distance = 0;
			    			}else {
			    				Document document = pincodeCollection.find(dbObject).first();
			    				distance = document.get("distance", Number.class); 
			    			}
			    			
			    			double margin = 0.02;
			    			Long finalPrice = Math.round(buyingPrice+(margin*buyingPrice)+distance.doubleValue());
			    			
			    			//Row finalPriceRow = RowFactory.create(finalPrice,quantity);
			    			StructType structType = new StructType();
			    			structType = structType.add("finalPrice", DataTypes.LongType, false);
			    			structType = structType.add("quantity", DataTypes.LongType, false);
			    			Object values[] = {finalPrice,quantity};
			    			Row finalPriceRow = new GenericRowWithSchema(values, structType);
			    			
			    			list.add(new Tuple2<Row, Row>(pinRow, finalPriceRow));
						}
						
					}
	    			//mongoclient.close();
					return list.iterator();
				}
			}).reduceByKey((priceRow1,priceRow2)->{
				
				Long finalPrice1 = priceRow1.getAs("finalPrice");
				Long finalPrice2 = priceRow2.getAs("finalPrice");
				
				if(finalPrice1.longValue()<finalPrice2.longValue())return priceRow1;
				return priceRow2;
			}).collect().forEach(tuple2->{
				
				MinPrice minPrice = new MinPrice();
				minPrice.setPrice(tuple2._2.getAs("finalPrice"));
				minPrice.setQuantity(tuple2._2.getAs("quantity"));
				
				bsons.add(Updates.set("prices."+tuple2._1.getInt(0), minPrice));
		        //System.out.println(tuple2._1.getInt(0)+","+tuple2._2.getAs("finalPrice"));
		      });
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// Write to mongodb
		CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), 
				fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		final MongoClient mongoclient = MongoClients.create("mongodb://localhost");
		MongoDatabase database = mongoclient.getDatabase("catalogue").withCodecRegistry(pojoCodecRegistry);
		MongoCollection<Document>productInfoCollection = database.getCollection("productinfos");
		Bson updates = Updates.combine(bsons);
		UpdateResult result = productInfoCollection.updateOne(Filters.eq("_id", new ObjectId(productId)), updates);
		System.out.println(result.getModifiedCount());
		
		System.out.println((System.currentTimeMillis())-start);
		
		//pincodes.rdd().cartesian(pincodePrices.rdd(), null).ta
		
//		destPincodes.forEach(row->{
//			Long start1 = System.currentTimeMillis();
//			Integer pin = row.getAs("pincode");
//			
//			String destPincodeStr = "destPin";
//			StructType structType = new StructType();
//        	structType = structType.add(destPincodeStr, DataTypes.IntegerType,false);
//        	
//        	List<Row>rows = new ArrayList<Row>();
// 			rows.add(RowFactory.create(pin));
// 			Dataset<Row>destPinDataset = sparkSession.createDataFrame(rows, structType).toDF();
// 			Long minPrice = helper(pin,pincodePrices, destPinDataset,productInfoDataset);
// 			map.put(pin, minPrice);
// 			Long end1 = System.currentTimeMillis();
// 			System.out.println(minPrice+"-"+(end1-start1));
//		});
//		Long end = System.currentTimeMillis();
//		System.out.println("Finished computation with "+(end-start));
//		System.out.println(map.size());
//		
//		logger.info("Computed min for product");
	}

	public Long helper(int destPincode, Dataset<Row> pincodePrices, Dataset<Row> destinationPinDF,
			Dataset<Row> productInfoDataset) {

		pincodePrices = pincodePrices.join(destinationPinDF);
		Row row = pincodePrices.filter(functions.col("leastPrice.buyingPrice").isNotNull())
				.filter(functions.col("leastPrice.quantity").$greater(0))
				.withColumn("finalPrice", functions.call_udf("distanceFunction", functions.col("leastPrice.pincodeNum"),
						functions.col("destPin"), functions.col("leastPrice.buyingPrice").cast(DataTypes.DoubleType)))
				.sort(functions.col("finalPrice")).limit(1).first();

		Long minPrice = row.getAs("finalPrice");

		StructType details = new StructType(
				new StructField[] { new StructField("price", DataTypes.LongType, false, Metadata.empty()) });
		String colName = "prices." + destPincode + "";
		StructType recordType = new StructType();
		recordType = recordType.add(colName, details, false);

		List<Row> l = new ArrayList<>();
		l.add(RowFactory.create(RowFactory.create(minPrice)));
		Dataset<Row> df = sparkSession.createDataFrame(l, recordType).toDF();
		productInfoDataset = productInfoDataset.join(df);

		WriteConfig writeConfig = SparkUtils.getWriteConfig(PRODUCT_INFO, sparkContext);
//   	    MongoSpark.save(productInfoDataset,writeConfig);
		pincodePrices = pincodePrices.drop("destPin").drop("finalPrice");
		return minPrice;
	}
}
