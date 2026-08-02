package com.ourgiant.dynamodb.browser.model;

import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;

import java.util.List;

// Helper class to store index information (primary index or a GSI) for the query dialog.
public class IndexOption {
    public final String name;
    public final String description;
    public final List<KeySchemaElement> keySchema;
    public final boolean isGSI;

    public IndexOption(String name, String description, List<KeySchemaElement> keySchema, boolean isGSI) {
        this.name = name;
        this.description = description;
        this.keySchema = keySchema;
        this.isGSI = isGSI;
    }

    @Override
    public String toString() {
        return description;
    }
}
