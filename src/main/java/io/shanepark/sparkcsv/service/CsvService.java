package io.shanepark.sparkcsv.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.File;

@Service
public class CsvService {

    private SparkSession spark;

    public CsvService() {
        spark = getSparkSession();
    }

    @PreDestroy
    public void close() {
        spark.close();
    }

    public File csvIntoParquet(File csvFile) {
        Dataset<Row> dataset = spark.read()
                .format("csv")
                .option("header", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load(csvFile.getAbsolutePath());

        for (String colName : dataset.columns()) {
            String newColName = renameColumnName(colName);
            dataset = dataset.withColumnRenamed(colName, newColName);
        }

        String parquetFileName = csvFile.getAbsolutePath().replace(".csv", ".parquet");

        dataset.write()
                .mode("overwrite")
                .parquet(parquetFileName);

        return new File(parquetFileName);
    }

    private static SparkSession getSparkSession() {
        SparkSession spark = SparkSession.builder()
                .config("spark.executor.memory", "2G")
                .appName("CsvIntoParquet")
                .master("local[*]")
                .getOrCreate();
        return spark;
    }

    private static String renameColumnName(String columnName) {
        StringBuilder sb = new StringBuilder();
        boolean lastWasUnderscore = false;

        if (Character.isDigit(columnName.charAt(0))) {
            sb.append('_');
        }

        for (char c : columnName.toCharArray()) {
            if (Character.isLetterOrDigit(c)) {
                sb.append(c);
                lastWasUnderscore = false;
                continue;
            }
            if (lastWasUnderscore) {
                continue;
            }

            sb.append('_');
            lastWasUnderscore = true;
        }

        if (lastWasUnderscore && sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

}
