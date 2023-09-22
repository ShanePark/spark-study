package io.shanepark.sparkcsv.controller;

import io.shanepark.sparkcsv.domain.CsvParseResult;
import io.shanepark.sparkcsv.service.CsvService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/csv")
@Slf4j
public class ApiController {

    private final CsvService csvService;

    @PostMapping
    public Long parseCsv(@RequestParam(name = "csv") MultipartFile csv) {
        log.info("csv: {},  size: {}", csv.getOriginalFilename(), csv.getSize());
        return csvService.askJob(csv);
    }

    @GetMapping
    public CsvParseResult getResult(@RequestParam(name = "jobId") Long jobId) {
        return csvService.getResult(jobId);
    }

}
