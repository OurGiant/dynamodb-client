package com.ourgiant.dynamodb.browser.model;

// DynamoDB Query only allows the partition key to be an exact match; the sort key can use
// any of these. (begins_with only applies to String/Binary sort keys - DynamoDB itself
// rejects it for a Number sort key, surfaced via the existing SdkException handling.)
public enum SortKeyOperator {
    EQ("="),
    LT("<"),
    LE("<="),
    GT(">"),
    GE(">="),
    BEGINS_WITH("begins with"),
    BETWEEN("between");

    private final String label;

    SortKeyOperator(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return label;
    }
}
