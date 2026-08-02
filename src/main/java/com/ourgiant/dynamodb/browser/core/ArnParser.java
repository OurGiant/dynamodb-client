package com.ourgiant.dynamodb.browser.core;

import com.ourgiant.dynamodb.browser.model.ParsedTableArn;

public final class ArnParser {

    private ArnParser() {
    }

    // ARN format: arn:aws:dynamodb:region:account:table/table-name
    public static ParsedTableArn parse(String arn) {
        String[] parts = arn.split(":");
        if (parts.length < 6) {
            throw new IllegalArgumentException("Invalid ARN format");
        }
        String region = parts[3];
        String name = parts[5].substring(parts[5].indexOf("/") + 1);
        return new ParsedTableArn(region, name);
    }
}
