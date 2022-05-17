package com.catalogue.pricing.service;

import com.catalogue.pricing.entities.Zone;

public interface PricingService {
	
	void computeMin(String productId,Zone zone,Integer destPincode);
	void computeMinForZone(Zone zone);
}
