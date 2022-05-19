package com.catalogue.pricing.test;

import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import static com.catalogue.pricing.commons.constants.CollectionConstants.PINCODE;
import static com.catalogue.pricing.commons.constants.CollectionConstants.PINCODE_DISTANCES;
import static com.catalogue.pricing.commons.constants.SparkConstants.APP_NAME;
import static com.catalogue.pricing.commons.constants.SparkConstants.COLLECTION;
import static com.catalogue.pricing.commons.constants.SparkConstants.SPARK_MONGO_INPUT_URI;
import static org.apache.spark.sql.functions.struct;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.sql.fieldTypes.api.java.ObjectId;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.catalogue.pricing.entities.DistanceMap;
import com.catalogue.pricing.entities.Person;

import org.apache.spark.sql.Encoder;
import com.catalogue.pricing.repository.DistanceRepository;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.MongoSpark;
import com.redislabs.provider.redis.ReadWriteConfig;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;

import scala.Function1;
import scala.Tuple2;
import scala.reflect.internal.Trees.Return;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import static org.apache.spark.sql.functions.udf;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MongoController implements Serializable{
    
	
	@Autowired
	transient JavaSparkContext sparkContext;
	
	@Autowired
	transient SparkConf sparkConf;
	
	@Autowired
	transient SparkSession sparkSession;
	
	@Autowired
	transient DistanceRepository distanceRepo;
	
	@Autowired
	transient RedisConfig redisConfig;
	
	@Autowired
	transient RedisContext redisContext;
	
	@Autowired
	transient RedisTemplate redisTemplate;

    @GetMapping("/redis")
    public void testMongo(){
    	
    	redisTemplate.setKeySerializer(new StringRedisSerializer());
    	redisTemplate.setValueSerializer(new StringRedisSerializer());
//    	redisTemplate.getConnectionFactory().getConnection().m
    	System.out.println(redisTemplate.opsForValue().get("504101_504273"));
//    	DistanceMap distanceMap = new DistanceMap();
//    	distanceMap.setDistance(30.00);
//    	distanceMap.setSrc_dest("chandan");
//    	
//    	distanceRepo.save(new DistanceMap());
//    	distanceRepo.findAll().forEach((k)->System.out.println(k));
      //System.out.println(distanceRepo.findById("504101_504273").get());
//    	ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkConf);
//    	System.out.println(redisContext.fromRedisKV("504101_504273", 0, redisConfig, readWriteConfig).distinct().toJavaRDD());
    }   
    
    @GetMapping("/southZone")
    public void category() {
    	
		Map<String, String>overrideMap = new HashMap<>();
    	overrideMap.put(COLLECTION, PINCODE_DISTANCES);
    	
    	ReadConfig readConfig = ReadConfig.create(sparkConf).withOptions(overrideMap);
    	Dataset<Row> pincodes = MongoSpark.load(sparkContext,readConfig).toDF();
    	
    	Dataset<DistanceMap>subpincodesDataset =  pincodes.limit(10).map((MapFunction<Row, DistanceMap>) v1 ->{
			
			DistanceMap distanceMap = new DistanceMap();
			distanceMap.setSrc_dest(String.valueOf(v1.getInt(7))+"_"+String.valueOf(v1.getInt(2)));
			distanceMap.setDistance(String.valueOf(v1.getDouble(4)));
			return distanceMap;
    		
		}, Encoders.bean(DistanceMap.class));
    	
    	subpincodesDataset.write().
    	format("org.apache.spark.sql.redis")
    	.option("table", "distance")
    	.option("key.column", "src_dest")
    	.mode(SaveMode.Overwrite)
    	.save();
    }
    
    
    
    @GetMapping(value = {"/prices/product/{pid}"})
    public void computeMinPrice(@PathVariable String pid,@RequestParam String zone) {
    	
    	
    	zone = zone.toLowerCase();
    	StringBuilder sBuilder = new StringBuilder();
    	sBuilder.append(zone);
    	sBuilder.append("zoneprices");
    	String collection = sBuilder.toString();
    	
    	Map<String, String>overrideMap = new HashMap<>();
    	overrideMap.put(COLLECTION, collection);
    	
    	//{"product": "ObjectId(sdfsdf)"}
    	ReadConfig readConfig = ReadConfig.create(sparkConf).withOptions(overrideMap);
    	Dataset<Row> zonePrices = MongoSpark.load(sparkContext,readConfig).withPipeline(Collections.singletonList(
                    Document.parse("{ $match : { product : ObjectId(\""+pid+"\")}}")
    		        )).toDF();
    	
    	
    	UserDefinedFunction  distanceFunction =  udf(new UDF3<Integer,Integer,Double, Double>() {

			@Override
			public Double call(Integer t1,Integer t2,Double sellingPrice) throws Exception {
				
    			final MongoClient mongoclient = MongoClients.create("mongodb://localhost");
    			MongoDatabase database = mongoclient.getDatabase("catalogue");
    			MongoCollection<Document>pincodeCollection = database.getCollection("pincodedistances");
    			
    			BasicDBObject dbObject = new BasicDBObject();
    			dbObject.append("sourcePincode", t1);
    			dbObject.append("destPincode", t2);

    			
    			Document document = pincodeCollection.find(dbObject).first();
    			double distance = document.getDouble("distance");
    			mongoclient.close();
    			return distance+sellingPrice;
			}
		}, DataTypes.DoubleType);
    	
    	distanceFunction = distanceFunction.asNonNullable();
    	sparkSession.udf().register("distanceFunction", distanceFunction);
    	
    	
    	overrideMap = new HashMap<>();
    	overrideMap.put(COLLECTION, "productinfos");
    	readConfig = ReadConfig.create(sparkConf).withOptions(overrideMap);
    	
    	Dataset<Row>productDataset = MongoSpark.load(sparkContext,readConfig).withPipeline(Collections.singletonList(
                Document.parse("{ $match : { _id : ObjectId(\""+pid+"\")}}")
		        )).toDF().limit(1);
    	
    	if(!productDataset.isEmpty()) {
    		
    		StructType details = new StructType(new StructField[]{
    			    new StructField("type", DataTypes.StringType, false, Metadata.empty()),
    			    new StructField("price", DataTypes.DoubleType, false, Metadata.empty())
    			   });
    		
    		
    		
    	    int destPincodes[] = {560079,504204,504296};
    	    productDataset = productDataset.select(productDataset.col("_id"));
    	    
        	for(int destPin: destPincodes) { // Needs to be changed, have to fetch destination pincodes from mongodb
        		
        		List<Row>list = new ArrayList<Row>();
            	list.add(RowFactory.create(destPin));
            	StructType structType = new StructType();
            	structType = structType.add(String.valueOf(destPin), DataTypes.IntegerType,false);
            	Dataset<Row>pinDF = sparkSession.createDataFrame(list, structType).toDF();
            	zonePrices = pinDF.join(zonePrices);
        		
            	try {
            		
            		zonePrices = zonePrices.
            					 filter(zonePrices.col("leastPrice.sellingPrice").isNotNull()).
            					 filter(zonePrices.col("leastPrice.quantity").$greater(0)).
            					 withColumn(String.valueOf(destPin), functions.call_udf("distanceFunction", 
					            				zonePrices.col("leastPrice.pincodeNum"),
					            				zonePrices.col(String.valueOf(destPin)),
					            				zonePrices.col("leastPrice.sellingPrice"))).
            					 sort(zonePrices.col(String.valueOf(destPin))).
            					 limit(1);
            		
            		Double minPrice = zonePrices.first().getAs(String.valueOf(destPin));
            		
            		String colName = "prices."+destPin+"";
            		StructType recordType = new StructType();
            		recordType = recordType.add(colName, details, false);
            		
            		
            		List<Row>l = new ArrayList<>();
            		l.add(RowFactory.create(RowFactory.create("FALCON",minPrice)));
            		Dataset<Row>df = sparkSession.createDataFrame(l, recordType).toDF();
            		productDataset = productDataset.join(df);
            		
            		Map<String, String> writeOverrides = new HashMap<String, String>();
            		writeOverrides.put("spark.mongodb.output.uri", "mongodb://127.0.0.1/catalogue.productinfos");
            		writeOverrides.put("collection", "productinfos");
            	    writeOverrides.put("writeConcern.w", "majority");
            	    writeOverrides.put("replaceDocument", "false");
//            	    
            	    WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
            	    MongoSpark.save(productDataset,writeConfig);
            		
            		
            		zonePrices = zonePrices.drop("destPin");
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
        	}
    	}
    }
    
    public double getDistance() {
    	return 33.5;
    }
    
    public void computeMin(String productId,int pincode,SparkConf sparkConf) {
    
    	StringBuilder queryBuilder = new StringBuilder();
    	queryBuilder.append("{");
    	queryBuilder.append("$match");
    	queryBuilder.append(":");
    	queryBuilder.append("{");
    	queryBuilder.append("product");
    	queryBuilder.append(":");
    	queryBuilder.append("ObjectId(\\\"");
    	queryBuilder.append(productId);
    	queryBuilder.append("\\\") } }");
    	
    	try {
    		Map<String, String>overrideMap = new HashMap<>();
        	overrideMap.put(COLLECTION, "southzoneprices");
    		ReadConfig rc = ReadConfig.create(sparkConf).withOptions(overrideMap);
    		
    		Dataset<Row> zonePrices =  MongoSpark.load(sparkContext,rc).withPipeline(Collections.singletonList(
    	              Document.parse(" { $match : { _id : ObjectId(\"6241f399ba42fdc0fa0eee75\") } }")
    			        )).toDF();
    	    	
    	    	zonePrices.limit(1).show();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    @GetMapping("/test1")
    @Async
    public void test() {
    	MapType mapType = new MapType(DataTypes.StringType,DataTypes.StringType,false);
    	
    	Map<String, String>overrideMap = new HashMap<>();
    	overrideMap.put(COLLECTION, PINCODE_DISTANCES);
    	
    	ReadConfig readConfig = ReadConfig.create(sparkConf).withOptions(overrideMap);
    	System.out.println("started");
    	try {
//    		JavaPairRDD<String, Double>distanceMap = MongoSpark.load(sparkContext,readConfig).withPipeline(Collections.singletonList(
//                    Document.parse(" { $match : { _id : ObjectId(\"6241f399ba42fdc0fa0eee75\") } }")
//    		        )).
//        			mapToPair(doc->
//        			new Tuple2<String, Double>(
//        					doc.getInteger("sourcePincode").toString()+"_"+doc.getInteger("destPincode").toString(), 
//        					doc.getDouble("distance").doubleValue())
//        			);
    		
    		
    		JavaPairRDD<String, Object>distanceMap = MongoSpark.load(sparkContext,readConfig).
        			mapToPair(doc->
        			new Tuple2<String, Object>(
        					doc.getInteger("sourcePincode").toString()+"_"+doc.getInteger("destPincode").toString(),
        					doc.get("distance")
        					)
        			);
    		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);

    		long start = System.currentTimeMillis();
    		System.out.println(distanceMap.lookup("504273_504204"));
    		long end = System.currentTimeMillis();
    		System.out.println("Time1"+(end-start)/1000);
    		
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
