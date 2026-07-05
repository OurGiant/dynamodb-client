package com.ourgiant.dynamodb.browser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JTextField;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryConditionBuildingTest {

    private DynamoDBBrowser browser;

    @BeforeEach
    void setUp() {
        browser = ReflectionSupport.newBrowserInstance();
        ReflectionSupport.setField(browser, "tableDescription", TableDescription.builder()
            .attributeDefinitions(
                AttributeDefinition.builder().attributeName("PK").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("SK").attributeType(ScalarAttributeType.S).build())
            .build());
    }

    private DynamoDBBrowser.IndexOption primaryIndex(KeySchemaElement... keySchema) {
        return new DynamoDBBrowser.IndexOption("Primary Index", "Primary Index", List.of(keySchema), false);
    }

    private DynamoDBBrowser.KeyConditionBuild build(DynamoDBBrowser.IndexOption indexOption, JPanel keyPanel) {
        return (DynamoDBBrowser.KeyConditionBuild) ReflectionSupport.invoke(browser, "buildKeyConditionExpression",
            new Class<?>[]{DynamoDBBrowser.IndexOption.class, JPanel.class}, indexOption, keyPanel);
    }

    private JTextField field(String name, String text) {
        JTextField field = new JTextField(text);
        field.setName(name);
        return field;
    }

    private JComboBox<DynamoDBBrowser.SortKeyOperator> operatorCombo(String attributeName,
            DynamoDBBrowser.SortKeyOperator selected) {
        JComboBox<DynamoDBBrowser.SortKeyOperator> combo = new JComboBox<>(DynamoDBBrowser.SortKeyOperator.values());
        combo.setName(attributeName + "__operator");
        combo.setSelectedItem(selected);
        return combo;
    }

    @Test
    void partitionKeyOnlyBuildsEqualityCondition() {
        DynamoDBBrowser.IndexOption index = primaryIndex(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build());

        JPanel keyPanel = new JPanel();
        keyPanel.add(field("PK", "user#1"));

        DynamoDBBrowser.KeyConditionBuild result = build(index, keyPanel);

        assertEquals("#k0 = :v0", result.expression());
        assertEquals("PK", result.names().get("#k0"));
        assertEquals("user#1", result.values().get(":v0").s());
    }

    @Test
    void missingPartitionKeyThrows() {
        DynamoDBBrowser.IndexOption index = primaryIndex(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build());

        JPanel keyPanel = new JPanel();
        keyPanel.add(field("PK", ""));

        assertThrows(IllegalArgumentException.class, () -> build(index, keyPanel));
    }

    @Test
    void sortKeyDefaultsToEqualityWhenNoOperatorComboPresent() {
        DynamoDBBrowser.IndexOption index = primaryIndex(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        JPanel keyPanel = new JPanel();
        keyPanel.add(field("PK", "user#1"));
        keyPanel.add(field("SK", "METADATA"));

        DynamoDBBrowser.KeyConditionBuild result = build(index, keyPanel);

        assertEquals("#k0 = :v0 AND #k1 = :v1", result.expression());
    }

    @Test
    void sortKeyIsOptionalWhenBlank() {
        DynamoDBBrowser.IndexOption index = primaryIndex(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        JPanel keyPanel = new JPanel();
        keyPanel.add(field("PK", "user#1"));
        keyPanel.add(field("SK", ""));
        keyPanel.add(operatorCombo("SK", DynamoDBBrowser.SortKeyOperator.BEGINS_WITH));

        DynamoDBBrowser.KeyConditionBuild result = build(index, keyPanel);

        assertEquals("#k0 = :v0", result.expression());
    }

    @Test
    void beginsWithBuildsFunctionCondition() {
        DynamoDBBrowser.IndexOption index = primaryIndex(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        JPanel keyPanel = new JPanel();
        keyPanel.add(field("PK", "tenant#1"));
        keyPanel.add(field("SK", "CLIENT#"));
        keyPanel.add(operatorCombo("SK", DynamoDBBrowser.SortKeyOperator.BEGINS_WITH));

        DynamoDBBrowser.KeyConditionBuild result = build(index, keyPanel);

        assertEquals("#k0 = :v0 AND begins_with(#k1, :v1)", result.expression());
        assertEquals("CLIENT#", result.values().get(":v1").s());
    }

    @Test
    void betweenBuildsTwoValuePlaceholders() {
        DynamoDBBrowser.IndexOption index = primaryIndex(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        JPanel keyPanel = new JPanel();
        keyPanel.add(field("PK", "tenant#1"));
        keyPanel.add(field("SK", "2026-01-01"));
        keyPanel.add(operatorCombo("SK", DynamoDBBrowser.SortKeyOperator.BETWEEN));
        keyPanel.add(field("SK__to", "2026-12-31"));

        DynamoDBBrowser.KeyConditionBuild result = build(index, keyPanel);

        assertEquals("#k0 = :v0 AND #k1 BETWEEN :v1 AND :v2", result.expression());
        assertEquals("2026-01-01", result.values().get(":v1").s());
        assertEquals("2026-12-31", result.values().get(":v2").s());
    }

    @Test
    void betweenWithoutSecondValueThrows() {
        DynamoDBBrowser.IndexOption index = primaryIndex(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        JPanel keyPanel = new JPanel();
        keyPanel.add(field("PK", "tenant#1"));
        keyPanel.add(field("SK", "2026-01-01"));
        keyPanel.add(operatorCombo("SK", DynamoDBBrowser.SortKeyOperator.BETWEEN));
        keyPanel.add(field("SK__to", ""));

        assertThrows(IllegalArgumentException.class, () -> build(index, keyPanel));
    }

    @Test
    void comparisonOperatorsBuildExpectedSymbols() {
        DynamoDBBrowser.IndexOption index = primaryIndex(
            KeySchemaElement.builder().attributeName("PK").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("SK").keyType(KeyType.RANGE).build());

        JPanel keyPanel = new JPanel();
        keyPanel.add(field("PK", "tenant#1"));
        keyPanel.add(field("SK", "100"));
        keyPanel.add(operatorCombo("SK", DynamoDBBrowser.SortKeyOperator.GE));

        DynamoDBBrowser.KeyConditionBuild result = build(index, keyPanel);

        assertEquals("#k0 = :v0 AND #k1 >= :v1", result.expression());
    }
}
