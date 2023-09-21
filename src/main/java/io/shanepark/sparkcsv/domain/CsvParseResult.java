package io.shanepark.sparkcsv.domain;

import lombok.Getter;

import java.util.List;

@Getter
public class CsvParseResult {
    private final List<ColumnData> graphs;
    private final List<List<String>> data;

    public CsvParseResult(List<ColumnData> graphs, List<List<String>> data) {
        this.graphs = graphs;
        this.data = data;
    }

}
