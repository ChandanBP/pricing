package com.catalogue.pricing.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.catalogue.pricing.entities.DistanceMap;

@Repository
public interface DistanceRepository extends CrudRepository<DistanceMap, String>{}
