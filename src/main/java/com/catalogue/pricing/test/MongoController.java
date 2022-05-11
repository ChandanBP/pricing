package com.catalogue.pricing.test;

import org.bson.Document;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.mongodb.spark.config.ReadConfig;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import org.apache.spark.sql.types.StructType;

import com.catalogue.pricing.entities.DistanceMap;
import com.catalogue.pricing.entities.Person;

import org.apache.spark.sql.Encoder;
import com.catalogue.pricing.repository.DistanceRepository;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.spark.MongoSpark;
import com.redislabs.provider.redis.ReadWriteConfig;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;

import scala.Function1;
import scala.Tuple2;
import scala.reflect.internal.Trees.Return;

import org.apache.spark.sql.api.java.UDF2;
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

import static com.catalogue.pricing.commons.SparkConstants.APP_NAME;
import static com.catalogue.pricing.commons.SparkConstants.COLLECTION;
import static com.catalogue.pricing.commons.SparkConstants.SPARK_MONGO_INPUT_URI;
import static com.catalogue.pricing.commons.CollectionConstants.PINCODE;
import static com.catalogue.pricing.commons.CollectionConstants.SOUTH_PINCODE_DISTANCES;

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
    	overrideMap.put(COLLECTION, SOUTH_PINCODE_DISTANCES);
    	
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
    
    
    @GetMapping(value = {"/price/product/{pid}"})
    public void computeMinPrice(@PathVariable String pid,@RequestParam String zone) {
    	
    	Map<String, String>overrideMap = new HashMap<>();
    	overrideMap.put(COLLECTION, PINCODE);
    	
    	ReadConfig readConfig = ReadConfig.create(sparkConf).withOptions(overrideMap);
    	Dataset<Row> pincodes = MongoSpark.load(sparkContext,readConfig).withPipeline(Collections.singletonList(
                    Document.parse(" { $match : { zone : \"South\" } }")
    		        )).toDF();
    	
    	pincodes.limit(1).show();
    	
    	zone = zone.toLowerCase();
    	StringBuilder sBuilder = new StringBuilder();
    	sBuilder.append(zone);
    	sBuilder.append("zoneprices");
    	String collection = sBuilder.toString();
    	
    	
    	overrideMap.put(COLLECTION, collection);
    	readConfig = readConfig.withOptions(overrideMap);
    	
    	
    	StringBuilder queryBuilder = new StringBuilder();
    	queryBuilder.append("{");
    	queryBuilder.append("$match");
    	queryBuilder.append(":");
    	queryBuilder.append("{");
    	queryBuilder.append("product");
    	queryBuilder.append(":");
    	queryBuilder.append("ObjectId(\\\"");
    	queryBuilder.append(pid);
    	queryBuilder.append("\\\") } }");
    	Dataset<Row> zonePrices =  MongoSpark.load(sparkContext,readConfig).withPipeline(Collections.singletonList(
	              Document.parse(" { $match : { product : ObjectId(\"6242b346ba42fdc0fab2bc8a\") } }")
			        )).toDF();
    	
    	
    	
    	UserDefinedFunction  distanceFunction =  udf(new UDF2<Object,String, Double>() {

    		
			@Override
			public Double call(Object t1,String t2) throws Exception {
				
//				sparkSession.read()
//				.format("org.apache.spark.sql.redis")
//				.option("table", "distance")
//				.option("key.column", "src_dest").load().show();
				
				return 72.5;
			}
		}, DataTypes.DoubleType);
    	distanceFunction = distanceFunction.asNonNullable();
    	sparkSession.udf().register("distanceFunction", distanceFunction);
    	
    	
    	List<Row>list = new ArrayList<Row>();
    	list.add(RowFactory.create("6242b346ba42fdc0fab2bc8a"));
    	StructType structType = new StructType();
    	structType = structType.add("destPin", DataTypes.StringType,false);
    	Dataset<Row>pinDF = sparkSession.createDataFrame(list, structType).toDF();
    	zonePrices = pinDF.join(zonePrices);

    	zonePrices.limit(10).foreach(row->{
    		
    		double distances[] = {138.15,35.77,166.93,17.09,25.44,87.66,110.54,54.55,43.21,78.12};
    		Random random = new Random();
    		double distance = distances[random.nextInt(distances.length)];
    		
    		List<Row>list1 = new ArrayList<Row>();
        	list.add(RowFactory.create(distance));
        	StructType st = new StructType();
        	
    	});
    	
//    	zonePrices.filter(zonePrices.col("leastPrice.sellingPrice").isNotNull()).
//    			   filter(zonePrices.col("leastPrice.quantity").$greater(0)).
//    			   withColumn("distance", functions.call_udf("distanceFunction", zonePrices.col("pincode"),zonePrices.col("destPin"))).
//    			   sort(zonePrices.col("leastPrice.sellingPrice")).
//    			   limit(1).
//    			   show();
    	
//    	UserDefinedFunction random = udf(new UDF2<String, String, Double>() {
//
//			@Override
//			public Double call(String t1, String t2) throws Exception {
//				// TODO Auto-generated method stub
//				System.out.println(t1);
//				System.out.println(t2);
//				return 35.77;
//			}
//    		
//		}, null);
//    	random.asNondeterministic();
//    	sparkSession.udf().register("random", random);    	
    	
//    	UserDefinedFunction random = udf((pin1,pin2)->Math.random(), 
//		DataTypes.DoubleType);
    	//Dataset<Integer> years = zonePrices.map((Function1<Row, Integer>) row -> row.<Integer>getAs("YEAR"), Encoders.INT());
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
    
    @GetMapping("/test")
    @Async
    public void test() {
    	MapType mapType = new MapType(DataTypes.StringType,DataTypes.StringType,false);
    	
    	Map<String, String>overrideMap = new HashMap<>();
    	overrideMap.put(COLLECTION, SOUTH_PINCODE_DISTANCES);
    	
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
