package com.ourgiant.dynamodb.browser.core;

import com.ourgiant.dynamodb.browser.model.KeyConditionBuild;
import com.ourgiant.dynamodb.browser.model.SortKeyOperator;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Builds a DynamoDB KeyConditionExpression from plain, already-extracted user input (no
// dependency on how that input was collected - the GUI layer reads text fields/combo boxes
// into the maps this takes). Throws IllegalArgumentException with a user-facing message for
// missing/invalid input.
public final class KeyConditionBuilder {

    private KeyConditionBuilder() {
    }

    // Looks up a key attribute's declared type from the table's attribute definitions, so query
    // key conditions can be built with the correct AttributeValue type (DynamoDB key comparisons
    // are type-strict: an S-typed condition never matches an N-typed key, and vice versa).
    // Defaults to String if the attribute isn't found (shouldn't happen for a real key schema).
    public static ScalarAttributeType resolveAttributeType(List<AttributeDefinition> attributeDefinitions,
            String attributeName) {
        for (AttributeDefinition definition : attributeDefinitions) {
            if (definition.attributeName().equals(attributeName)) {
                return definition.attributeType();
            }
        }
        return ScalarAttributeType.S;
    }

    public static AttributeValue buildKeyAttributeValue(ScalarAttributeType type, String rawValue) {
        if (type == ScalarAttributeType.N) {
            return AttributeValue.builder().n(rawValue).build();
        }
        if (type == ScalarAttributeType.B) {
            throw new IllegalArgumentException(
                "Binary key attributes aren't supported for querying in this UI.");
        }
        return AttributeValue.builder().s(rawValue).build();
    }

    // values: attributeName -> entered value (partition key's, or sort key's main value).
    // operators: attributeName -> selected SortKeyOperator (sort keys only; defaults to EQ if absent).
    // toValues: attributeName -> the second ("and:") value, used only for BETWEEN.
    public static KeyConditionBuild build(List<KeySchemaElement> keySchema,
            List<AttributeDefinition> attributeDefinitions, Map<String, String> values,
            Map<String, SortKeyOperator> operators, Map<String, String> toValues) {
        List<String> conditions = new java.util.ArrayList<>();
        Map<String, String> names = new HashMap<>();
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        int placeholderIndex = 0;

        for (KeySchemaElement key : keySchema) {
            String attributeName = key.attributeName();
            String rawValue = values.getOrDefault(attributeName, "").trim();

            String namePlaceholder = "#k" + placeholderIndex;
            String valuePlaceholder = ":v" + placeholderIndex;

            if (key.keyType() == KeyType.HASH) {
                if (rawValue.isEmpty()) {
                    throw new IllegalArgumentException("Please provide the partition key.");
                }
                names.put(namePlaceholder, attributeName);
                expressionValues.put(valuePlaceholder,
                    buildKeyAttributeValue(resolveAttributeType(attributeDefinitions, attributeName), rawValue));
                conditions.add(namePlaceholder + " = " + valuePlaceholder);
                placeholderIndex++;
                continue;
            }

            // Sort key is optional.
            if (rawValue.isEmpty()) {
                continue;
            }

            SortKeyOperator operator = operators.getOrDefault(attributeName, SortKeyOperator.EQ);

            names.put(namePlaceholder, attributeName);
            expressionValues.put(valuePlaceholder,
                buildKeyAttributeValue(resolveAttributeType(attributeDefinitions, attributeName), rawValue));
            placeholderIndex++;

            if (operator == SortKeyOperator.BETWEEN) {
                String toValue = toValues.getOrDefault(attributeName, "").trim();
                if (toValue.isEmpty()) {
                    throw new IllegalArgumentException("Please provide both values for a BETWEEN condition.");
                }
                String toPlaceholder = ":v" + placeholderIndex;
                expressionValues.put(toPlaceholder,
                    buildKeyAttributeValue(resolveAttributeType(attributeDefinitions, attributeName), toValue));
                placeholderIndex++;
                conditions.add(namePlaceholder + " BETWEEN " + valuePlaceholder + " AND " + toPlaceholder);
            } else if (operator == SortKeyOperator.BEGINS_WITH) {
                conditions.add("begins_with(" + namePlaceholder + ", " + valuePlaceholder + ")");
            } else {
                conditions.add(namePlaceholder + " " + operator + " " + valuePlaceholder);
            }
        }

        if (conditions.isEmpty()) {
            throw new IllegalArgumentException("Please provide at least the partition key.");
        }

        return new KeyConditionBuild(String.join(" AND ", conditions), names, expressionValues);
    }
}
