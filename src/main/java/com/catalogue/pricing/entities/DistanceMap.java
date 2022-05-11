package com.catalogue.pricing.entities;

import java.io.Serializable;

import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;

@RedisHash("distance")
public class DistanceMap implements Serializable{
	
    private String id;
	private String src_dest;
	private String distance;
	
	public String getSrc_dest() {
		return src_dest;
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public void setSrc_dest(String src_dest) {
		this.src_dest = src_dest;
	}
	
	public String getDistance() {
		return distance;
	}
	
	public void setDistance(String distance) {
		this.distance = distance;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuilder sBuilder = new StringBuilder();
		sBuilder.append(src_dest);
		sBuilder.append(",");
		sBuilder.append(distance);
		return sBuilder.toString();
	}
}
