package com.ourgiant.dynamodb.browser.core;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class AttributeValueFormatter {

    private AttributeValueFormatter() {
    }

    public static String format(AttributeValue value) {
        if (value == null) return "";
        if (value.s() != null) return value.s();
        if (value.n() != null) return value.n();
        if (value.bool() != null) return value.bool().toString();
        if (value.nul() != null && value.nul()) return "NULL";
        if (value.hasSs()) return value.ss().toString();
        if (value.hasNs()) return value.ns().toString();
        if (value.hasM()) return "{Map}";
        if (value.hasL()) return "[List]";
        return value.toString();
    }

    public static String formatDetailed(AttributeValue value) {
        if (value == null) return "null";
        if (value.s() != null) return "\"" + value.s() + "\"";
        if (value.n() != null) return value.n();
        if (value.bool() != null) return value.bool().toString();
        if (value.nul() != null && value.nul()) return "NULL";
        if (value.hasSs()) return "String Set: " + value.ss();
        if (value.hasNs()) return "Number Set: " + value.ns();
        if (value.hasM()) return "Map: " + value.m().toString();
        if (value.hasL()) return "List: " + value.l().toString();
        if (value.b() != null) return "Binary: " + value.b().toString();
        return value.toString();
    }
}
