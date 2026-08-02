package com.ourgiant.dynamodb.browser.model;

// ARN format: arn:aws:dynamodb:region:account:table/table-name
public record ParsedTableArn(String region, String tableName) {
}
