package com.ourgiant.dynamodb.browser.core;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

// Pure logic behind the records grid: which columns to show and how to format a record's
// values into a row, kept separate from the JTable/DefaultTableModel that actually displays
// them (see gui.RecordsPanel).
public final class RecordGridModel {

    private static final String PK_SUFFIX = " (PK)";

    private RecordGridModel() {
    }

    // Show only the primary key column(s) in the summary grid - every item has these,
    // regardless of shape, so the columns are stable across refreshes and don't depend on
    // which item happens to load first. Single-table-design schemas can have wildly
    // different attributes per item; cramming a union of all of them into the grid turns
    // out to be more noise than signal in practice. Full detail (any attribute, for one
    // record) is still available via double-click -> Record Details.
    public static List<String> keyColumnNames(List<KeySchemaElement> keySchema) {
        List<String> columnNames = new ArrayList<>();
        for (KeySchemaElement key : keySchema) {
            columnNames.add(key.attributeName() + PK_SUFFIX);
        }
        return columnNames;
    }

    public static String attributeNameForColumn(String columnName) {
        return columnName.replace(PK_SUFFIX, "");
    }

    public static List<String> formatRow(List<String> columnNames, Map<String, AttributeValue> item) {
        List<String> row = new ArrayList<>();
        for (String columnName : columnNames) {
            AttributeValue value = item.get(attributeNameForColumn(columnName));
            row.add(AttributeValueFormatter.format(value));
        }
        return row;
    }

    // Shows a few example values for the given key attribute, drawn from whatever's currently
    // loaded, so the user has some idea what to type instead of pure guesswork (single-table-design
    // PK/SK values are often prefixed/composite, e.g. "CLIENT#123").
    public static List<String> sampleValues(List<Map<String, AttributeValue>> records, String attributeName,
            int maxSamples) {
        LinkedHashSet<String> samples = new LinkedHashSet<>();
        for (Map<String, AttributeValue> record : records) {
            AttributeValue value = record.get(attributeName);
            if (value != null) {
                samples.add(AttributeValueFormatter.format(value));
            }
            if (samples.size() >= maxSamples) {
                break;
            }
        }
        return new ArrayList<>(samples);
    }
}
