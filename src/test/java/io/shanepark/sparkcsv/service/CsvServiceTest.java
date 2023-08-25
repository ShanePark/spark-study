package io.shanepark.sparkcsv.service;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

class CsvServiceTest {

    CsvService csvService = new CsvService();

    // now it doesn't work over 10000 rows yet
    final String TEST_CSV_PATH = "/home/shane/Downloads/test_1000.csv";

    @Test
    void csvIntoParquet() {
        File csvFile = new File(TEST_CSV_PATH);
        File parquetFile = csvService.csvIntoParquet(csvFile);
        assertThat(parquetFile).exists();
    }

}
