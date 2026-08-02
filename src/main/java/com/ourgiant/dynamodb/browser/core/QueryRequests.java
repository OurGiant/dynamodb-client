package com.ourgiant.dynamodb.browser.core;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

import java.util.Map;

public final class QueryRequests {

    private QueryRequests() {
    }

    public static QueryRequest build(String tableName, int limit, String keyConditionExpression,
            Map<String, String> names, Map<String, AttributeValue> values, String indexName,
            Map<String, AttributeValue> exclusiveStartKey) {
        QueryRequest.Builder builder = QueryRequest.builder()
            .tableName(tableName)
            .limit(limit)
            .keyConditionExpression(keyConditionExpression)
            .expressionAttributeNames(names)
            .expressionAttributeValues(values);

        if (indexName != null) {
            builder.indexName(indexName);
        }
        if (exclusiveStartKey != null) {
            builder.exclusiveStartKey(exclusiveStartKey);
        }
        return builder.build();
    }
}
