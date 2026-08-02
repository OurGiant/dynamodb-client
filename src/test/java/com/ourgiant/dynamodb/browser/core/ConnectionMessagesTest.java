package com.ourgiant.dynamodb.browser.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectionMessagesTest {

    @Test
    void titleIsPlainWhenNotConnected() {
        assertEquals("DynamoDB Browser", ConnectionMessages.windowTitle(null, null, null, null));
    }

    @Test
    void titleIncludesProfileAccountRegionAndTable() {
        assertEquals("DynamoDB Browser — example-profile (123456789012, us-east-1) — Orders",
            ConnectionMessages.windowTitle("example-profile", "123456789012", "us-east-1", "Orders"));
    }

    @Test
    void titleDegradesGracefullyWithoutAccountId() {
        assertEquals("DynamoDB Browser — example-profile (us-east-1) — Orders",
            ConnectionMessages.windowTitle("example-profile", null, "us-east-1", "Orders"));
    }

    @Test
    void deleteMessageIncludesTableProfileAndAccount() {
        assertEquals(
            "Are you sure you want to delete this record from \"Orders\" "
                + "(profile: example-profile, account: 123456789012)?\nThis action cannot be undone.",
            ConnectionMessages.deleteConfirmationMessage("Orders", "example-profile", "123456789012"));
    }

    @Test
    void deleteMessageDegradesGracefullyWithNoConnectionContext() {
        assertEquals(
            "Are you sure you want to delete this record?\nThis action cannot be undone.",
            ConnectionMessages.deleteConfirmationMessage(null, null, null));
    }
}
