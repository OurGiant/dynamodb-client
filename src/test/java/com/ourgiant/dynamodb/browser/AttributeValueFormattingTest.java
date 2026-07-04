package com.ourgiant.dynamodb.browser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AttributeValueFormattingTest {

    private DynamoDBBrowser browser;

    @BeforeEach
    void setUp() {
        browser = ReflectionSupport.newBrowserInstance();
    }

    private String format(AttributeValue value) {
        return (String) ReflectionSupport.invoke(browser, "formatAttributeValue",
            new Class<?>[]{AttributeValue.class}, value);
    }

    private String formatDetailed(AttributeValue value) {
        return (String) ReflectionSupport.invoke(browser, "formatAttributeValueDetailed",
            new Class<?>[]{AttributeValue.class}, value);
    }

    @Test
    void formatsNullValue() {
        assertEquals("", format(null));
        assertEquals("null", formatDetailed(null));
    }

    @Test
    void formatsString() {
        AttributeValue value = AttributeValue.builder().s("hello").build();
        assertEquals("hello", format(value));
        assertEquals("\"hello\"", formatDetailed(value));
    }

    @Test
    void formatsNumber() {
        AttributeValue value = AttributeValue.builder().n("42").build();
        assertEquals("42", format(value));
        assertEquals("42", formatDetailed(value));
    }

    @Test
    void formatsBoolean() {
        AttributeValue value = AttributeValue.builder().bool(true).build();
        assertEquals("true", format(value));
        assertEquals("true", formatDetailed(value));
    }

    @Test
    void formatsNull_dynamoDbNullType() {
        AttributeValue value = AttributeValue.builder().nul(true).build();
        assertEquals("NULL", format(value));
        assertEquals("NULL", formatDetailed(value));
    }

    @Test
    void formatsStringSet() {
        AttributeValue value = AttributeValue.builder().ss("a", "b").build();
        assertEquals(List.of("a", "b").toString(), format(value));
        assertEquals("String Set: " + List.of("a", "b"), formatDetailed(value));
    }

    @Test
    void formatsNumberSet() {
        AttributeValue value = AttributeValue.builder().ns("1", "2").build();
        assertEquals(List.of("1", "2").toString(), format(value));
        assertEquals("Number Set: " + List.of("1", "2"), formatDetailed(value));
    }

    @Test
    void formatsMap() {
        AttributeValue value = AttributeValue.builder()
            .m(Map.of("k", AttributeValue.builder().s("v").build()))
            .build();
        assertEquals("{Map}", format(value));
        assertEquals("Map: " + value.m(), formatDetailed(value));
    }

    @Test
    void formatsList() {
        AttributeValue value = AttributeValue.builder()
            .l(List.of(AttributeValue.builder().s("v").build()))
            .build();
        assertEquals("[List]", format(value));
        assertEquals("List: " + value.l(), formatDetailed(value));
    }

    @Test
    void formatsBinary_detailedOnly() {
        AttributeValue value = AttributeValue.builder()
            .b(SdkBytes.fromUtf8String("bytes"))
            .build();
        assertEquals("Binary: " + value.b(), formatDetailed(value));
    }
}
