package com.catalogue.pricing.entities;

import java.io.Serializable;

public class MinPrice implements Serializable{

	Long price;
	Integer quantity;
	
	public Long getPrice() {
		return price;
	}
	public void setPrice(Long price) {
		this.price = price;
	}
	public Integer getQuantity() {
		return quantity;
	}
	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}
}
