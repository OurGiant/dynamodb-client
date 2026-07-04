package com.ourgiant.dynamodb.browser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryKeyTypeTest {

    private DynamoDBBrowser browser;

    @BeforeEach
    void setUp() {
        browser = ReflectionSupport.newBrowserInstance();
    }

    private ScalarAttributeType resolveAttributeType(String attributeName) {
        return (ScalarAttributeType) ReflectionSupport.invoke(browser, "resolveAttributeType",
            new Class<?>[]{String.class}, attributeName);
    }

    private AttributeValue buildKeyAttributeValue(ScalarAttributeType type, String rawValue) {
        return (AttributeValue) ReflectionSupport.invoke(browser, "buildKeyAttributeValue",
            new Class<?>[]{ScalarAttributeType.class, String.class}, type, rawValue);
    }

    @Test
    void resolvesNumberTypeFromTableDescription() {
        TableDescription description = TableDescription.builder()
            .attributeDefinitions(
                AttributeDefinition.builder().attributeName("orderId").attributeType(ScalarAttributeType.N).build(),
                AttributeDefinition.builder().attributeName("customerName").attributeType(ScalarAttributeType.S).build())
            .build();
        ReflectionSupport.setField(browser, "tableDescription", description);

        assertEquals(ScalarAttributeType.N, resolveAttributeType("orderId"));
        assertEquals(ScalarAttributeType.S, resolveAttributeType("customerName"));
    }

    @Test
    void resolvesToStringWhenAttributeNotFound() {
        TableDescription description = TableDescription.builder()
            .attributeDefinitions(List.of())
            .build();
        ReflectionSupport.setField(browser, "tableDescription", description);

        assertEquals(ScalarAttributeType.S, resolveAttributeType("unknownAttribute"));
    }

    @Test
    void buildsNumberAttributeValueForNumberType() {
        AttributeValue value = buildKeyAttributeValue(ScalarAttributeType.N, "42");

        assertEquals("42", value.n());
        assertNull(value.s());
    }

    @Test
    void buildsStringAttributeValueForStringType() {
        AttributeValue value = buildKeyAttributeValue(ScalarAttributeType.S, "abc-123");

        assertEquals("abc-123", value.s());
        assertNull(value.n());
    }

    @Test
    void rejectsBinaryKeyType() {
        assertThrows(IllegalArgumentException.class,
            () -> buildKeyAttributeValue(ScalarAttributeType.B, "ignored"));
    }
}
