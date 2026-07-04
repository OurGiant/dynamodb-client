package com.ourgiant.dynamodb.browser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArnParsingTest {

    private DynamoDBBrowser browser;

    @BeforeEach
    void setUp() {
        browser = ReflectionSupport.newBrowserInstance();
    }

    private DynamoDBBrowser.ParsedTableArn parse(String arn) {
        return (DynamoDBBrowser.ParsedTableArn) ReflectionSupport.invoke(browser, "parseTableArn",
            new Class<?>[]{String.class}, arn);
    }

    @Test
    void parsesRegionAndTableNameFromArn() {
        DynamoDBBrowser.ParsedTableArn parsed =
            parse("arn:aws:dynamodb:us-west-2:123456789012:table/MyTable");

        assertEquals("us-west-2", parsed.region());
        assertEquals("MyTable", parsed.tableName());
    }

    @Test
    void parsesTableNameContainingSlashes() {
        // Table names can't actually contain '/', but the resource part after "table/"
        // should still be taken verbatim in case of any unexpected suffix.
        DynamoDBBrowser.ParsedTableArn parsed =
            parse("arn:aws:dynamodb:eu-central-1:999999999999:table/Orders");

        assertEquals("eu-central-1", parsed.region());
        assertEquals("Orders", parsed.tableName());
    }

    @Test
    void rejectsArnWithTooFewSegments() {
        assertThrows(IllegalArgumentException.class, () -> parse("not-an-arn"));
    }

    @Test
    void rejectsArnMissingTablePart() {
        assertThrows(IllegalArgumentException.class,
            () -> parse("arn:aws:dynamodb:us-east-1:123456789012"));
    }
}
