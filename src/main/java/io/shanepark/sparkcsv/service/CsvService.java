package io.shanepark.sparkcsv.service;

import io.shanepark.sparkcsv.database.ResultDB;
import io.shanepark.sparkcsv.domain.ColumnData;
import io.shanepark.sparkcsv.domain.CsvParseResult;
import io.shanepark.sparkcsv.domain.enums.DataType;
import io.shanepark.sparkcsv.websocket.WebsocketHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class CsvService {

    private final SparkSession spark = getSparkSession();
    private final ObjectProvider<CsvService> csvServiceProvider;
    private final AtomicLong idGenerator = new AtomicLong(0);
    private final WebsocketHandler websocketHandler;
    private final ResultDB resultDB;

    public long askJob(MultipartFile csv) {
        long jobId = idGenerator.getAndIncrement();
        CsvService service = csvServiceProvider.getObject();
        service.makeGraphAndData(jobId, csv);
        return jobId;
    }

    public CsvParseResult getResult(long jobId) {
        return resultDB.getResult(jobId);
    }

    @Async
    public void makeGraphAndData(long jobId, MultipartFile csv) {
        try {
            File tmpFile = File.createTempFile("tmp", ".csv");
            csv.transferTo(tmpFile);

            Dataset<Row> dataset = makeDataset(tmpFile);
            List<ColumnData> graphs = parseCsv(dataset);
            File parquetFile = makeParquet(dataset, csv.getOriginalFilename());
            List<List<String>> data = readParquet(parquetFile, 0, 100);

            CsvParseResult result = CsvParseResult.success(graphs, data);
            resultDB.finishJob(jobId, result);
        } catch (IOException e) {
            CsvParseResult result = CsvParseResult.error(e.getMessage());
            resultDB.finishJob(jobId, result);
        }
        websocketHandler.sendComplete(jobId);
    }

    private List<List<String>> readParquet(File parquetFile, int page, int size) {
        Dataset<Row> dataset = spark.read().parquet(parquetFile.getAbsolutePath());

        dataset.createOrReplaceTempView("parquetFile");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquetFile LIMIT " + size + " OFFSET " + page * size);

        return sqlDF.collectAsList().stream()
                .map(row -> {
                    List<String> rowList = new ArrayList<>();
                    for (int i = 0; i < row.length(); i++) {
                        Object data = row.get(i);
                        rowList.add(data == null ? "" : data.toString());
                    }
                    return rowList;
                }).collect(Collectors.toList());
    }

    private List<ColumnData> parseCsv(Dataset<Row> dataset) {
        List<ColumnData> result = Arrays.stream(dataset.columns())
                .map(column -> makeColumnData(column, dataset))
                .collect(Collectors.toList());
        return result;
    }

    private File makeParquet(Dataset<Row> dataset, String originalFileName) {
        File parquetFile = new File("/tmp/" + originalFileName + ".parquet");

        if (parquetFile.exists()) {
            log.info("parquet file already exists: {}", parquetFile.getAbsolutePath());
            return parquetFile;
        }

        dataset.write()
                .mode("overwrite")
                .parquet(parquetFile.getAbsolutePath());

        return parquetFile;
    }

    private Dataset<Row> makeDataset(File csvFile) {
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

    private ColumnData makeColumnData(String column, Dataset<Row> dataset) {
        String typeName = dataset.schema().apply(column).dataType().typeName();
        DataType type = DataType.from(typeName);
        long distinctCount = dataset.select(column).distinct().count();
        long rowSize = dataset.count();

        ColumnData columnData = new ColumnData(column, distinctCount, rowSize, type);
        columnData.graphData = makeGraphData(column, dataset, columnData);

        return columnData;
    }

    private Map<String, Long> makeGraphData(String column, Dataset<Row> dataset, ColumnData columnData) {
        switch (columnData.graphType) {
            case PIE:
            case ALL_SAME:
                return makeCountMap(column, dataset);
            case ALL_UNIQUE:
                return null;
            case TOP4:
                return makeTop4Map(column, dataset);
            case COLUMN:
                return makeColumnChart(columnData, column, dataset);
            default:
                throw new RuntimeException("Unknown graph type: " + columnData.graphType);
        }
    }

    private Map<String, Long> makeColumnChart(ColumnData columnData, String column, Dataset<Row> dataset) {
        long distinctCount = columnData.distinctCount;
        if (distinctCount < 10) {
            return makeCountMap(column, dataset);
        }

        // 숫자인 경우 10 구간으로 나눈다.
        if (columnData.dataType == DataType.NUMBER) {
            Double min = Double.parseDouble(dataset.select(functions.min(column)).first().get(0).toString());
            Double max = Double.parseDouble(dataset.select(functions.max(column)).first().get(0).toString());

            double roundedMin = Math.floor(min / 10) * 10;
            double roundedMax = Math.ceil(max / 10) * 10;
            double interval = (roundedMax - roundedMin) / 10;

            List<Row> sectionRows = new ArrayList<>();
            for (double i = roundedMin; i <= roundedMax; i += interval) {
                sectionRows.add(RowFactory.create(i));
            }
            StructType sectionSchema = new StructType().add("section", DataTypes.DoubleType);
            Dataset<Row> sectionDF = dataset.sparkSession().createDataFrame(sectionRows, sectionSchema);

            Dataset<Row> resultDF = sectionDF.join(
                            dataset.withColumn("section", functions.floor(functions.col(column).divide(interval)).multiply(interval).plus(roundedMin)),
                            "section",
                            "left_outer"
                    )
                    .groupBy("section")
                    .count();

            Map<String, Long> resultMap = resultDF.collectAsList().stream()
                    .sorted(Comparator.comparing(row -> row.getDouble(0)))
                    .collect(Collectors.toMap(
                            row -> String.format("%.1f", row.getDouble(0)),
                            row -> row.getLong(1),
                            (a, b) -> a, LinkedHashMap::new)
                    );
            return resultMap;
        }

        // 날짜인 경우 24 구간으로 나눈다.
        LocalDate max = LocalDate.parse(dataset.select(functions.max(column)).first().get(0).toString());
        LocalDate min = LocalDate.parse(dataset.select(functions.min(column)).first().get(0).toString());
        long daysBetween = ChronoUnit.DAYS.between(min, max);

        long interval = daysBetween / 24;

        // Create an alias for the date group
        String aliasName = "date_section";

        // Group by the truncated date
        Map<String, Long> resultMap = dataset
                .withColumn(aliasName, functions.datediff(functions.lit(max), functions.col(column)).divide(interval).cast("int"))
                .groupBy(aliasName)
                .count()
                .collectAsList()
                .stream()
                .collect(LinkedHashMap::new, (m, r) -> m.put(min.plusDays(r.getInt(0) * interval).toString(), r.getLong(1)), LinkedHashMap::putAll);

        for (int i = 0; i < 24; i++) {
            String key = min.plusDays(i * interval).toString();
            resultMap.putIfAbsent(key, 0L);
        }

        resultMap = resultMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));

        return resultMap;
    }

    private Map<String, Long> makeTop4Map(String column, Dataset<Row> dataset) {
        List<Row> rows = dataset
                .groupBy(column)
                .count()
                .orderBy(functions.col("count").desc())
                .limit(4)
                .sort(functions.col("count").desc())
                .collectAsList();

        return rows.stream()
                .collect(LinkedHashMap::new, (m, r) -> {
                    Object key = r.get(0);
                    m.put(key == null ? "" : key.toString(), r.getLong(1));
                }, LinkedHashMap::putAll);
    }

    private HashMap<String, Long> makeCountMap(String column, Dataset<Row> dataset) {
        return dataset
                .groupBy(column)
                .count()
                .sort(functions.col(column).asc())
                .collectAsList()
                .stream()
                .collect(HashMap::new, (m, r) -> {
                    Object key = r.get(0);
                    m.put(key == null ? "" : key.toString(), r.getLong(1));
                }, HashMap::putAll);
    }

    private SparkSession getSparkSession() {
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .config("spark.executor.memory", "2g")
                .appName("CsvIntoParquet")
                .getOrCreate();
        return spark;
    }

    private String renameColumnName(String columnName) {
        StringBuilder sb = new StringBuilder();
        boolean lastWasUnderscore = false;

        if (Character.isDigit(columnName.charAt(0)) || columnName.isEmpty()) {
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
