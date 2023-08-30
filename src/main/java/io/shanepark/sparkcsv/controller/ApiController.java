package io.shanepark.sparkcsv.controller;

import io.shanepark.sparkcsv.domain.ColumnData;
import io.shanepark.sparkcsv.service.CsvService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/csv")
@Slf4j
public class ApiController {

    private final CsvService csvService;

    @PostMapping
    public List<ColumnData> parseCsv(@RequestParam(name = "csv") MultipartFile csv) throws IOException {
        log.info("csv: {},  size: {}", csv.getOriginalFilename(), csv.getSize());

        File tmpFile = File.createTempFile("tmp", ".csv");
        csv.transferTo(tmpFile);

        List<ColumnData> result = csvService.parseCsv(tmpFile);
        return result;
    }

    @GetMapping("/estimate")
    public long estimateTime(@RequestParam(name = "file_size") long fileSize) {
        return csvService.estimateTime(fileSize);
    }

}
