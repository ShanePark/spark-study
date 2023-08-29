package io.shanepark.sparkcsv.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CsvService {

    public String csvIntoParquet(File csvFile) {
        Dataset<Row> dataset = dataset(csvFile);

        String[] columns = dataset.columns();
        StringBuilder sb = new StringBuilder();

        for (String column : columns) {
            Dataset<Row> distinct = dataset.select(column).distinct();
            long distinctCount = distinct.count();
            sb.append("column = " + column + ", distinctCount = " + distinctCount + "\n");
        }

        return sb.toString();
    }

    private Dataset<Row> dataset(File csvFile) {
        SparkSession spark = getSparkSession();

        // 1. read csv file
        Dataset<Row> dataset = spark.read()
                .format("csv")
                .option("header", true)
                .option("multiline", true)
                .option("inferSchema", true)
                .load(csvFile.getAbsolutePath());

        // 2. rename columns
        StructField[] fields = dataset.schema().fields();
        String[] alias = new String[fields.length];
        Map<String, Integer> fieldCnt = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            String original = fields[i].name();
            String renamed = renameColumnName(original);
            Integer cnt = fieldCnt.merge(renamed, 1, Integer::sum);
            if (cnt > 1) {
                renamed = renamed + "_" + cnt;
            }
            alias[i] = renamed;
        }


        Dataset<Row> df = dataset.toDF(alias);
        return df;
    }

    private static SparkSession getSparkSession() {
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .config("spark.executor.memory", "2G")
                .appName("CsvIntoParquet")
                .getOrCreate();
        return spark;
    }

    public static String renameColumnName(String columnName) {
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
