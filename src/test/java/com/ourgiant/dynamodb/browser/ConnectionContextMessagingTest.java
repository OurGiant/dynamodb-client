package com.ourgiant.dynamodb.browser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectionContextMessagingTest {

    private DynamoDBBrowser browser;

    @BeforeEach
    void setUp() {
        browser = ReflectionSupport.newBrowserInstance();
    }

    private String buildWindowTitle() {
        return (String) ReflectionSupport.invoke(browser, "buildWindowTitle", new Class<?>[]{});
    }

    private String buildDeleteConfirmationMessage() {
        return (String) ReflectionSupport.invoke(browser, "buildDeleteConfirmationMessage", new Class<?>[]{});
    }

    @Test
    void titleIsPlainWhenNotConnected() {
        assertEquals("DynamoDB Browser", buildWindowTitle());
    }

    @Test
    void titleIncludesProfileAccountRegionAndTable() {
        ReflectionSupport.setField(browser, "connectedProfile", "example-profile");
        ReflectionSupport.setField(browser, "connectedAccountId", "123456789012");
        ReflectionSupport.setField(browser, "connectedRegion", "us-east-1");
        ReflectionSupport.setField(browser, "tableName", "Orders");

        assertEquals("DynamoDB Browser — example-profile (123456789012, us-east-1) — Orders", buildWindowTitle());
    }

    @Test
    void titleDegradesGracefullyWithoutAccountId() {
        ReflectionSupport.setField(browser, "connectedProfile", "example-profile");
        ReflectionSupport.setField(browser, "connectedRegion", "us-east-1");
        ReflectionSupport.setField(browser, "tableName", "Orders");

        assertEquals("DynamoDB Browser — example-profile (us-east-1) — Orders", buildWindowTitle());
    }

    @Test
    void deleteMessageIncludesTableProfileAndAccount() {
        ReflectionSupport.setField(browser, "connectedProfile", "example-profile");
        ReflectionSupport.setField(browser, "connectedAccountId", "123456789012");
        ReflectionSupport.setField(browser, "tableName", "Orders");

        assertEquals(
            "Are you sure you want to delete this record from \"Orders\" "
                + "(profile: example-profile, account: 123456789012)?\nThis action cannot be undone.",
            buildDeleteConfirmationMessage());
    }

    @Test
    void deleteMessageDegradesGracefullyWithNoConnectionContext() {
        assertEquals(
            "Are you sure you want to delete this record?\nThis action cannot be undone.",
            buildDeleteConfirmationMessage());
    }
}
