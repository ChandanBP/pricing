package com.catalogue.pricing.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.catalogue.pricing.entities.Zone;
import com.catalogue.pricing.service.PricingService;

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
}
