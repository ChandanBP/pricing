package com.catalogue.pricing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class PricingApplication {

	public static void main(String[] args) {
		try {
			SpringApplication.run(PricingApplication.class, args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
