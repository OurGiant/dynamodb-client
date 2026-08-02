package com.ourgiant.dynamodb.browser.model;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

public record KeyConditionBuild(String expression, Map<String, String> names, Map<String, AttributeValue> values) {
}
