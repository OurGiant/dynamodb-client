package com.ourgiant.dynamodb.browser.core;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexDescriptionsTest {

    @Test
    void describesHashOnlyIndex() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build());

        assertEquals("Primary Index (id:PK)", IndexDescriptions.buildIndexDescription("Primary Index", keySchema));
    }

    @Test
    void describesHashAndRangeIndex() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("customerId").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("orderDate").keyType(KeyType.RANGE).build());

        assertEquals("byCustomer (customerId:PK, orderDate:SK)",
            IndexDescriptions.buildIndexDescription("byCustomer", keySchema));
    }
}
