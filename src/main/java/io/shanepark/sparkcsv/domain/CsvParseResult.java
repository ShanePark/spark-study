package io.shanepark.sparkcsv.domain;

import lombok.Getter;

import java.util.List;

@Getter
public class CsvParseResult {
    private final List<ColumnData> graphs;
    private final List<List<String>> data;
    private final boolean success;
    private final String error;

    public CsvParseResult(List<ColumnData> graphs, List<List<String>> data, boolean success, String error) {
        this.graphs = graphs;
        this.data = data;
        this.success = success;
        this.error = error;
    }

    static public CsvParseResult error(String error) {
        return new CsvParseResult(null, null, false, error);
    }

    static public CsvParseResult success(List<ColumnData> graphs, List<List<String>> data) {
        return new CsvParseResult(graphs, data, true, null);
    }

}
