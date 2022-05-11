package com.catalogue.pricing.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;

import static com.catalogue.pricing.commons.SparkConstants.SPARK_MONGO_INPUT_URI;
import static com.catalogue.pricing.commons.SparkConstants.APP_NAME;

@Configuration
public class SparkConfig {
	
	@Value("${mongo_url}")
    private String mongoURL;
	
	@Bean
	public SparkConf sparkConf() {
		return new SparkConf()
				 .setMaster("local")
				 .set(SPARK_MONGO_INPUT_URI, "mongodb://127.0.0.1/catalogue.categories")
				 .setAppName(APP_NAME);
	}
	
	@Bean
	public JavaSparkContext javaSparkContext() {
		return new JavaSparkContext(sparkSession().sparkContext());
	}
	
	@Bean
	public RedisContext redisContext() {
		return new RedisContext(javaSparkContext().sc());
	}
	
	@Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
        	      .master("local")
        	      .appName(APP_NAME)
        	      .config(SPARK_MONGO_INPUT_URI, "mongodb://127.0.0.1/catalogue.categories")
        	      .getOrCreate();
    }
	
	@Bean
	public RedisConfig redisConfig() {
		return RedisConfig.fromSparkConf(sparkConf());
	}
}