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
		return new ResponseEntity<String>("OK", HttpStatus.OK);
	}
}
