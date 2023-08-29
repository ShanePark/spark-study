package io.shanepark.sparkcsv.service;

import java.io.File;

class CsvServiceTest {

    static final String TEST_CSV_PATH = "/home/shane/Downloads/test_1000.csv";

    public static void main(String[] args) {
        CsvService csvService = new CsvService();
        File csvFile = new File(TEST_CSV_PATH);

        String result = csvService.csvIntoParquet(csvFile);

        System.err.printf(result);
    }

}
