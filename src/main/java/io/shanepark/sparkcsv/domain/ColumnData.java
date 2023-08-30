package io.shanepark.sparkcsv.domain;

import io.shanepark.sparkcsv.domain.enums.DataType;
import io.shanepark.sparkcsv.domain.enums.GraphType;

import java.util.Map;

public class ColumnData {
    public final String columnName;
    public final long distinctCount;
    public final DataType dataType;
    public final GraphType graphType;
    public Map<String, Long> graphData;

    public ColumnData(String columnName, long distinctCount, long rowSize, DataType dataType) {
        this.columnName = columnName;
        this.distinctCount = distinctCount;
        this.dataType = dataType;

        graphType = determineGraphType(distinctCount, rowSize, dataType);
    }

    private GraphType determineGraphType(long distinctCount, long rowSize, DataType dataType) {
        if (distinctCount == rowSize) {
            return GraphType.ALL_UNIQUE;
        }

        if (distinctCount == 1) {
            return GraphType.ALL_SAME;
        }

        if (distinctCount <= 3) {
            return GraphType.PIE;
        }

        if (dataType == DataType.NUMBER || dataType == DataType.DATE) {
            return GraphType.COLUMN;
        }

        return GraphType.TOP4;
    }

    @Override
    public String toString() {
        return "ColumnData{" +
                "columnName='" + columnName + '\'' +
                ", distinctCount=" + distinctCount +
                ", dataType=" + dataType +
                ", graphType=" + graphType +
                ", valueCountMap=" + graphData +
                '}';
    }
}
