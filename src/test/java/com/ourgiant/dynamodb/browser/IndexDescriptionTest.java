package com.ourgiant.dynamodb.browser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexDescriptionTest {

    private DynamoDBBrowser browser;

    @BeforeEach
    void setUp() {
        browser = ReflectionSupport.newBrowserInstance();
    }

    private String describe(String indexName, List<KeySchemaElement> keySchema) {
        return (String) ReflectionSupport.invoke(browser, "buildIndexDescription",
            new Class<?>[]{String.class, List.class}, indexName, keySchema);
    }

    @Test
    void describesHashOnlyIndex() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build());

        assertEquals("Primary Index (id:PK)", describe("Primary Index", keySchema));
    }

    @Test
    void describesHashAndRangeIndex() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("customerId").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("orderDate").keyType(KeyType.RANGE).build());

        assertEquals("byCustomer (customerId:PK, orderDate:SK)", describe("byCustomer", keySchema));
    }
}
