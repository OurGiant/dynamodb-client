package com.ourgiant.dynamodb.browser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import javax.swing.table.DefaultTableModel;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableColumnSetupTest {

    private DynamoDBBrowser browser;
    private DefaultTableModel tableModel;

    @BeforeEach
    void setUp() {
        browser = ReflectionSupport.newBrowserInstance();
        tableModel = new DefaultTableModel();
        ReflectionSupport.setField(browser, "tableModel", tableModel);
    }

    private void setupTableColumns() {
        ReflectionSupport.invoke(browser, "setupTableColumns", new Class<?>[]{});
    }

    private void addRecordToTable(Map<String, AttributeValue> item) {
        ReflectionSupport.invoke(browser, "addRecordToTable", new Class<?>[]{Map.class}, item);
    }

    private static AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    @Test
    void setupTableColumnsAddsOnlyPrimaryKeyColumns() {
        TableDescription description = TableDescription.builder()
            .keySchema(
                KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build())
            .build();
        ReflectionSupport.setField(browser, "tableDescription", description);

        setupTableColumns();

        assertEquals(2, tableModel.getColumnCount());
        assertEquals("PK (PK)", tableModel.getColumnName(0));
        assertEquals("SK (PK)", tableModel.getColumnName(1));
    }

    @Test
    void columnsStayFixedToKeysRegardlessOfItemShape() {
        // Single-table-design tables can have wildly different attributes per item; the grid
        // should stay limited to the stable primary key columns rather than growing per item
        // (full detail for a given record is available via the Record Details dialog instead).
        TableDescription description = TableDescription.builder()
            .keySchema(KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build())
            .build();
        ReflectionSupport.setField(browser, "tableDescription", description);
        setupTableColumns();

        addRecordToTable(Map.of("PK", s("user#1"), "name", s("Alice")));
        addRecordToTable(Map.of("PK", s("order#1"), "total", s("42"), "items", s("3")));

        assertEquals(1, tableModel.getColumnCount());
        assertEquals("PK (PK)", tableModel.getColumnName(0));
        assertEquals(2, tableModel.getRowCount());
        assertEquals("user#1", tableModel.getValueAt(0, 0));
        assertEquals("order#1", tableModel.getValueAt(1, 0));
    }
}
