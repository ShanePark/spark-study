package io.shanepark.sparkcsv.service;

import io.shanepark.sparkcsv.database.ResultDB;
import io.shanepark.sparkcsv.domain.ColumnData;
import io.shanepark.sparkcsv.domain.CsvParseResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@SpringBootTest
class CsvServiceTest {

    // Sections by date data
    static final String TEST_CSV_PATH = "/home/shane/Downloads/csv/test_1000.csv";

    // sections by number data
//    static final String TEST_CSV_PATH = "/home/shane/Downloads/csv/grades.csv";

    @Autowired
    CsvService csvService;

    @Autowired
    ResultDB resultDB;

    private static final Logger log = LoggerFactory.getLogger(CsvServiceTest.class);

    @Test
    public void test() throws IOException, InterruptedException {
        File csvFile = new File(TEST_CSV_PATH);

        try (InputStream is = new FileInputStream(csvFile)) {
            MultipartFile multipartFile = new MockMultipartFile("test.csv", is);

            long jobId = csvService.askJob(multipartFile);

            int seconds = 0;
            while (!resultDB.hasFinished(jobId)) {
                log.warn("waiting for job to finish... " + seconds++ + "s");
                Thread.sleep(1000);

                if (seconds > 300)
                    throw new RuntimeException("Job took too long");
            }

            CsvParseResult result = csvService.getResult(jobId);

            List<ColumnData> graphs = result.getGraphs();
            List<List<String>> data = result.getData();
            log.warn("=== Graphs ===");
            for (ColumnData graph : graphs) {
                log.warn("{}", graph);
            }
            log.warn("=== Data ===");
            for (List<String> datum : data) {
                log.warn("{}", datum);
            }
        }
    }

}
