package io.shanepark.sparkcsv.service;

import io.shanepark.sparkcsv.domain.ColumnData;
import io.shanepark.sparkcsv.domain.CsvParseResult;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

class CsvServiceTest {

    // Sections by date data
    static final String TEST_CSV_PATH = "/home/shane/Downloads/csv/test_1000.csv";

    // sections by number data
//    static final String TEST_CSV_PATH = "/home/shane/Downloads/csv/grades.csv";

    public static void main(String[] args) throws IOException {
        CsvService csvService = new CsvService();
        File csvFile = new File(TEST_CSV_PATH);

        try (InputStream is = new FileInputStream(csvFile)) {
            MultipartFile multipartFile = new MockMultipartFile("test.csv", is);

            CsvParseResult result = csvService.getGraphAndData(multipartFile);
            List<ColumnData> graphs = result.getGraphs();
            List<List<String>> data = result.getData();
            System.out.println("=== Graphs ===");
            for (ColumnData graph : graphs) {
                System.out.println(graph);
            }
            System.out.println("=== Data ===");
            for (List<String> datum : data) {
                System.out.println(datum);
            }
        }
    }

}
