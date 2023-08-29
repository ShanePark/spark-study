package io.shanepark.sparkcsv.service;

import io.shanepark.sparkcsv.domain.ColumnData;

import java.io.File;
import java.util.List;

class CsvServiceTest {

    // Sections by date data
    static final String TEST_CSV_PATH = "/home/shane/Downloads/test_1000.csv";

    // sections by number data
//    static final String TEST_CSV_PATH = "/home/shane/Downloads/grades.csv";

    public static void main(String[] args) {
        CsvService csvService = new CsvService();
        File csvFile = new File(TEST_CSV_PATH);

        List<ColumnData> list = csvService.parseCsv(csvFile);
        for (ColumnData columnData : list) {
            System.err.println(columnData);
        }
    }

}
