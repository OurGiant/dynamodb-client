package com.ourgiant.dynamodb.browser.core;

import com.ourgiant.dynamodb.browser.model.KeyConditionBuild;
import com.ourgiant.dynamodb.browser.model.SortKeyOperator;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KeyConditionBuilderTest {

    private static final List<AttributeDefinition> ATTRIBUTE_DEFINITIONS = List.of(
        AttributeDefinition.builder().attributeName("PK").attributeType(ScalarAttributeType.S).build(),
        AttributeDefinition.builder().attributeName("SK").attributeType(ScalarAttributeType.S).build());

    private KeyConditionBuild build(List<KeySchemaElement> keySchema, Map<String, String> values,
            Map<String, SortKeyOperator> operators, Map<String, String> toValues) {
        return KeyConditionBuilder.build(keySchema, ATTRIBUTE_DEFINITIONS, values, operators, toValues);
    }

    @Test
    void partitionKeyOnlyBuildsEqualityCondition() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build());

        KeyConditionBuild result = build(keySchema, Map.of("PK", "user#1"), Map.of(), Map.of());

        assertEquals("#k0 = :v0", result.expression());
        assertEquals("PK", result.names().get("#k0"));
        assertEquals("user#1", result.values().get(":v0").s());
    }

    @Test
    void missingPartitionKeyThrows() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build());

        assertThrows(IllegalArgumentException.class, () -> build(keySchema, Map.of("PK", ""), Map.of(), Map.of()));
    }

    @Test
    void sortKeyDefaultsToEqualityWhenNoOperatorProvided() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        KeyConditionBuild result = build(keySchema,
            Map.of("PK", "user#1", "SK", "METADATA"), Map.of(), Map.of());

        assertEquals("#k0 = :v0 AND #k1 = :v1", result.expression());
    }

    @Test
    void sortKeyIsOptionalWhenBlank() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        KeyConditionBuild result = build(keySchema,
            Map.of("PK", "user#1", "SK", ""), Map.of("SK", SortKeyOperator.BEGINS_WITH), Map.of());

        assertEquals("#k0 = :v0", result.expression());
    }

    @Test
    void beginsWithBuildsFunctionCondition() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        KeyConditionBuild result = build(keySchema,
            Map.of("PK", "tenant#1", "SK", "CLIENT#"), Map.of("SK", SortKeyOperator.BEGINS_WITH), Map.of());

        assertEquals("#k0 = :v0 AND begins_with(#k1, :v1)", result.expression());
        assertEquals("CLIENT#", result.values().get(":v1").s());
    }

    @Test
    void betweenBuildsTwoValuePlaceholders() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        KeyConditionBuild result = build(keySchema,
            Map.of("PK", "tenant#1", "SK", "2026-01-01"), Map.of("SK", SortKeyOperator.BETWEEN),
            Map.of("SK", "2026-12-31"));

        assertEquals("#k0 = :v0 AND #k1 BETWEEN :v1 AND :v2", result.expression());
        assertEquals("2026-01-01", result.values().get(":v1").s());
        assertEquals("2026-12-31", result.values().get(":v2").s());
    }

    @Test
    void betweenWithoutSecondValueThrows() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        assertThrows(IllegalArgumentException.class, () -> build(keySchema,
            Map.of("PK", "tenant#1", "SK", "2026-01-01"), Map.of("SK", SortKeyOperator.BETWEEN),
            Map.of("SK", "")));
    }

    @Test
    void comparisonOperatorsBuildExpectedSymbols() {
        List<KeySchemaElement> keySchema = List.of(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        KeyConditionBuild result = build(keySchema,
            Map.of("PK", "tenant#1", "SK", "100"), Map.of("SK", SortKeyOperator.GE), Map.of());

        assertEquals("#k0 = :v0 AND #k1 >= :v1", result.expression());
    }

    @Test
    void resolvesNumberTypeFromAttributeDefinitions() {
        List<AttributeDefinition> definitions = List.of(
            AttributeDefinition.builder().attributeName("orderId").attributeType(ScalarAttributeType.N).build(),
            AttributeDefinition.builder().attributeName("customerName").attributeType(ScalarAttributeType.S).build());

        assertEquals(ScalarAttributeType.N, KeyConditionBuilder.resolveAttributeType(definitions, "orderId"));
        assertEquals(ScalarAttributeType.S, KeyConditionBuilder.resolveAttributeType(definitions, "customerName"));
    }

    @Test
    void resolvesToStringWhenAttributeNotFound() {
        assertEquals(ScalarAttributeType.S,
            KeyConditionBuilder.resolveAttributeType(List.of(), "unknownAttribute"));
    }

    @Test
    void buildsNumberAttributeValueForNumberType() {
        var value = KeyConditionBuilder.buildKeyAttributeValue(ScalarAttributeType.N, "42");

        assertEquals("42", value.n());
        assertNull(value.s());
    }

    @Test
    void buildsStringAttributeValueForStringType() {
        var value = KeyConditionBuilder.buildKeyAttributeValue(ScalarAttributeType.S, "abc-123");

        assertEquals("abc-123", value.s());
        assertNull(value.n());
    }

    @Test
    void rejectsBinaryKeyType() {
        assertThrows(IllegalArgumentException.class,
            () -> KeyConditionBuilder.buildKeyAttributeValue(ScalarAttributeType.B, "ignored"));
    }
}
