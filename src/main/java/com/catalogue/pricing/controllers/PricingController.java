package com.catalogue.pricing.controllers;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.catalogue.pricing.entities.MinPrice;
import com.catalogue.pricing.entities.Zone;
import com.catalogue.pricing.service.PricingService;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.LinkedList;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

@RestController
public class PricingController {
	
	@Autowired
	PricingService pricingService;
	
	@GetMapping(value = {"/price/product/{pid}/{zone}/{destPincode}"})
	public ResponseEntity<String> computeMin(@PathVariable String pid,
											 @PathVariable Zone zone,
											 @PathVariable int destPincode) {
		
		pricingService.computeMin(pid,zone, destPincode);
		return new ResponseEntity<String>("Computing min price...", HttpStatus.OK);
	}
	
	@GetMapping(value = {"/price/product/{pid}/{zone}"})
	public ResponseEntity<String> computeMinForZone(@PathVariable String pid,@PathVariable Zone zone) {
		pricingService.computeMinForZone(pid, zone);
		return new ResponseEntity<String>("Computing min price...", HttpStatus.OK);
	}
	
	@GetMapping(value = {"/test"})
	public ResponseEntity<String> test() {
		
		CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), 
				fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		final MongoClient mongoclient = MongoClients.create("mongodb://localhost");
		MongoDatabase database = mongoclient.getDatabase("catalogue").withCodecRegistry(pojoCodecRegistry);
		
		MongoCollection<Document>pincodeCollection = database.getCollection("pincodedistances");
		MongoCollection<Document>productInfoCollection = database.getCollection("productinfos");
		
		MinPrice minPrice1 = new MinPrice();
		minPrice1.setPrice(2000L);
		minPrice1.setQuantity(10);
		
		MinPrice minPrice2 = new MinPrice();
		minPrice2.setPrice(3000L);
		minPrice2.setQuantity(15);
		
		
		List<Bson>bsons = new LinkedList<>();
		for(int i=0;i<2;i++) {
			if(i==0) {
				bsons.add(Updates.set("prices.560079", minPrice1));
			}else {
				bsons.add(Updates.set("prices.560001", minPrice2));
			}
		}

		Bson updates = Updates.combine(bsons);
		
		
		try {
			UpdateResult result = productInfoCollection.updateOne(Filters.eq("_id", new ObjectId("6242b346ba42fdc0fab2bc8a")), updates);
			System.out.println(result.getModifiedCount());
			
		} catch (Exception e) {
			
		}
        
        
		return new ResponseEntity<String>("Computing min price...", HttpStatus.OK);
	}
}
