package com.ourgiant.dynamodb.browser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SampleAttributeValuesTest {

    private DynamoDBBrowser browser;

    @BeforeEach
    void setUp() {
        browser = ReflectionSupport.newBrowserInstance();
    }

    @SuppressWarnings("unchecked")
    private List<String> sampleAttributeValues(List<Map<String, AttributeValue>> records, String attributeName, int max) {
        ReflectionSupport.setField(browser, "allRecords", new ArrayList<>(records));
        return (List<String>) ReflectionSupport.invoke(browser, "sampleAttributeValues",
            new Class<?>[]{String.class, int.class}, attributeName, max);
    }

    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    @Test
    void returnsEmptyWhenNoRecordsLoaded() {
        List<String> samples = sampleAttributeValues(List.of(), "PK", 3);

        assertTrue(samples.isEmpty());
    }

    @Test
    void returnsDistinctValuesUpToMax() {
        List<Map<String, AttributeValue>> records = List.of(
            Map.of("PK", s("CLIENT#1")),
            Map.of("PK", s("CLIENT#1")), // duplicate, shouldn't be counted twice
            Map.of("PK", s("CLIENT#2")),
            Map.of("PK", s("CLIENT#3")),
            Map.of("PK", s("CLIENT#4")));

        List<String> samples = sampleAttributeValues(records, "PK", 3);

        assertEquals(3, samples.size());
        assertEquals(List.of("CLIENT#1", "CLIENT#2", "CLIENT#3"), samples);
    }

    @Test
    void ignoresRecordsMissingTheAttribute() {
        List<Map<String, AttributeValue>> records = List.of(
            Map.of("SK", s("METADATA")),
            Map.of("PK", s("CLIENT#1"), "SK", s("METADATA")));

        List<String> samples = sampleAttributeValues(records, "PK", 3);

        assertEquals(List.of("CLIENT#1"), samples);
    }
}
