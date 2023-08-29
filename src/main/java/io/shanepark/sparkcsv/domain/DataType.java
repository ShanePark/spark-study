package io.shanepark.sparkcsv.domain;

public enum DataType {
    NUMBER, STRING, DATE, BOOLEAN;

    public static DataType from(String s) {
        switch (s) {
            case "integer":
            case "double":
                return NUMBER;
            case "date":
                return DATE;
            case "boolean":
                return BOOLEAN;
            case "string":
            default:
                return STRING;
        }
    }

}
