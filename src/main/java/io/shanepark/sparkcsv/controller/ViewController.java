package io.shanepark.sparkcsv.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class ViewController {

    @GetMapping
    public String main() {
        return "index.html";
    }

}
