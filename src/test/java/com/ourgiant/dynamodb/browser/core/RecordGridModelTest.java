package com.ourgiant.dynamodb.browser.core;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordGridModelTest {

    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    @Test
    void keyColumnNamesAddsPkSuffixPerKeySchemaElement() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        assertEquals(List.of("PK (PK)", "SK (PK)"), RecordGridModel.keyColumnNames(keySchema));
    }

    @Test
    void formatRowStaysFixedToKeyColumnsRegardlessOfItemShape() {
        // Single-table-design tables can have wildly different attributes per item; the grid
        // should stay limited to the stable primary key columns rather than growing per item
        // (full detail for a given record is available via the Record Details dialog instead).
        List<String> columnNames = List.of("PK (PK)");

        assertEquals(List.of("user#1"),
            RecordGridModel.formatRow(columnNames, Map.of("PK", s("user#1"), "name", s("Alice"))));
        assertEquals(List.of("order#1"),
            RecordGridModel.formatRow(columnNames, Map.of("PK", s("order#1"), "total", s("42"))));
    }

    @Test
    void returnsEmptyWhenNoRecordsLoaded() {
        assertTrue(RecordGridModel.sampleValues(List.of(), "PK", 3).isEmpty());
    }

    @Test
    void returnsDistinctValuesUpToMax() {
        List<Map<String, AttributeValue>> records = List.of(
            Map.of("PK", s("CLIENT#1")),
            Map.of("PK", s("CLIENT#1")), // duplicate, shouldn't be counted twice
            Map.of("PK", s("CLIENT#2")),
            Map.of("PK", s("CLIENT#3")),
            Map.of("PK", s("CLIENT#4")));

        List<String> samples = RecordGridModel.sampleValues(records, "PK", 3);

        assertEquals(3, samples.size());
        assertEquals(List.of("CLIENT#1", "CLIENT#2", "CLIENT#3"), samples);
    }

    @Test
    void ignoresRecordsMissingTheAttribute() {
        List<Map<String, AttributeValue>> records = List.of(
            Map.of("SK", s("METADATA")),
            Map.of("PK", s("CLIENT#1"), "SK", s("METADATA")));

        assertEquals(List.of("CLIENT#1"), RecordGridModel.sampleValues(records, "PK", 3));
    }
}
