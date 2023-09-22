package io.shanepark.sparkcsv.database;

import io.shanepark.sparkcsv.domain.CsvParseResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class ResultDB {

    private final ConcurrentHashMap<Long, CsvParseResult> resultMap = new ConcurrentHashMap<>();

    public boolean hasFinished(long id) {
        return resultMap.containsKey(id);
    }

    public void finishJob(long jobId, CsvParseResult result) {
        resultMap.put(jobId, result);
    }

    public CsvParseResult getResult(long jobId) {
        return resultMap.get(jobId);
    }
}
