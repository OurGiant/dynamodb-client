package com.ourgiant.dynamodb.browser.core;

import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;

import java.util.ArrayList;
import java.util.List;

public final class IndexDescriptions {

    private IndexDescriptions() {
    }

    public static String buildIndexDescription(String indexName, List<KeySchemaElement> keySchema) {
        StringBuilder desc = new StringBuilder(indexName);
        desc.append(" (");

        List<String> keyParts = new ArrayList<>();
        for (KeySchemaElement key : keySchema) {
            String keyType = key.keyType() == KeyType.HASH ? "PK" : "SK";
            keyParts.add(key.attributeName() + ":" + keyType);
        }
        desc.append(String.join(", ", keyParts));
        desc.append(")");

        return desc.toString();
    }
}
