package com.akash.BigQueryAppDemo.controller;


import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class BigQueryController {

    @GetMapping("/api")
    public ResponseEntity<Object> getApi(){

        return new ResponseEntity<>("Success", HttpStatus.OK);
    }
}
