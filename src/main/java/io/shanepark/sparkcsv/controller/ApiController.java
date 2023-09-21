package io.shanepark.sparkcsv.controller;

import io.shanepark.sparkcsv.domain.CsvParseResult;
import io.shanepark.sparkcsv.service.CsvService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/csv")
@Slf4j
public class ApiController {

    private final CsvService csvService;

    @PostMapping
    public CsvParseResult parseCsv(@RequestParam(name = "csv") MultipartFile csv) throws IOException {
        log.info("csv: {},  size: {}", csv.getOriginalFilename(), csv.getSize());
        return csvService.getGraphAndData(csv);
    }

}
