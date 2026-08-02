package com.ourgiant.dynamodb.browser.core;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AttributeValueFormatterTest {

    @Test
    void formatsNullValue() {
        assertEquals("", AttributeValueFormatter.format(null));
        assertEquals("null", AttributeValueFormatter.formatDetailed(null));
    }

    @Test
    void formatsString() {
        AttributeValue value = AttributeValue.builder().s("hello").build();
        assertEquals("hello", AttributeValueFormatter.format(value));
        assertEquals("\"hello\"", AttributeValueFormatter.formatDetailed(value));
    }

    @Test
    void formatsNumber() {
        AttributeValue value = AttributeValue.builder().n("42").build();
        assertEquals("42", AttributeValueFormatter.format(value));
        assertEquals("42", AttributeValueFormatter.formatDetailed(value));
    }

    @Test
    void formatsBoolean() {
        AttributeValue value = AttributeValue.builder().bool(true).build();
        assertEquals("true", AttributeValueFormatter.format(value));
        assertEquals("true", AttributeValueFormatter.formatDetailed(value));
    }

    @Test
    void formatsNull_dynamoDbNullType() {
        AttributeValue value = AttributeValue.builder().nul(true).build();
        assertEquals("NULL", AttributeValueFormatter.format(value));
        assertEquals("NULL", AttributeValueFormatter.formatDetailed(value));
    }

    @Test
    void formatsStringSet() {
        AttributeValue value = AttributeValue.builder().ss("a", "b").build();
        assertEquals(List.of("a", "b").toString(), AttributeValueFormatter.format(value));
        assertEquals("String Set: " + List.of("a", "b"), AttributeValueFormatter.formatDetailed(value));
    }

    @Test
    void formatsNumberSet() {
        AttributeValue value = AttributeValue.builder().ns("1", "2").build();
        assertEquals(List.of("1", "2").toString(), AttributeValueFormatter.format(value));
        assertEquals("Number Set: " + List.of("1", "2"), AttributeValueFormatter.formatDetailed(value));
    }

    @Test
    void formatsMap() {
        AttributeValue value = AttributeValue.builder()
            .m(Map.of("k", AttributeValue.builder().s("v").build()))
            .build();
        assertEquals("{Map}", AttributeValueFormatter.format(value));
        assertEquals("Map: " + value.m(), AttributeValueFormatter.formatDetailed(value));
    }

    @Test
    void formatsList() {
        AttributeValue value = AttributeValue.builder()
            .l(List.of(AttributeValue.builder().s("v").build()))
            .build();
        assertEquals("[List]", AttributeValueFormatter.format(value));
        assertEquals("List: " + value.l(), AttributeValueFormatter.formatDetailed(value));
    }

    @Test
    void formatsBinary_detailedOnly() {
        AttributeValue value = AttributeValue.builder()
            .b(SdkBytes.fromUtf8String("bytes"))
            .build();
        assertEquals("Binary: " + value.b(), AttributeValueFormatter.formatDetailed(value));
    }
}
