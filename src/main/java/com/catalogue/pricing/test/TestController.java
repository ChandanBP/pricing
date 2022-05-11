package com.catalogue.pricing.test;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {
    
    

    @GetMapping(value = {"/error"})
    public String error(){
        return "error";
    }

    @GetMapping(value = {"/hello"})
    public String hello(){
        return "Hi there";
    }
}
