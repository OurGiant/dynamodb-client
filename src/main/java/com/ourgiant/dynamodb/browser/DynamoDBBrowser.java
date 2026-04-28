package com.ourgiant.dynamodb.browser;

import javax.swing.*;
import javax.swing.table.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.util.*;
import java.util.List;
import java.util.prefs.Preferences;
import java.awt.image.BufferedImage;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.core.exception.SdkException;

public class DynamoDBBrowser extends JFrame {
    private final Preferences prefs = Preferences.userNodeForPackage(DynamoDBBrowser.class);
    private DynamoDbClient dynamoDb;
    private String tableName;
    private String tableArn;
    private JTable recordsTable;
    private DefaultTableModel tableModel;
    private JButton loadMoreButton;
    private Map<String, AttributeValue> lastEvaluatedKey;
    private final List<Map<String, AttributeValue>> allRecords = new ArrayList<>();
    private TableDescription tableDescription;
    private final Integer dynamoQueryLimit = (Integer) 50;

    private Image createAppIcon() {
        BufferedImage icon = new BufferedImage(16, 16, BufferedImage.TYPE_INT_RGB);
        Graphics2D g2d = icon.createGraphics();
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2d.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g2d.setColor(new Color(70, 130, 180));
        g2d.fillRect(0, 0, 16, 16);
        g2d.setColor(Color.WHITE);
        g2d.setFont(new Font("Arial", Font.BOLD, 10));
        g2d.drawString("D", 5, 12);
        g2d.dispose();
        return icon;
    }
  
    private void setupMenuBar() {
        JMenuBar menuBar = new JMenuBar();
        
        JMenu fileMenu = new JMenu("File");
        fileMenu.setMnemonic(KeyEvent.VK_F);
        
        fileMenu.addSeparator();
        
        JMenuItem exitItem = new JMenuItem("Exit");
        exitItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q, 
            Toolkit.getDefaultToolkit().getMenuShortcutKeyMaskEx()));
        exitItem.addActionListener(e -> System.exit(0));
        fileMenu.add(exitItem);
        
        menuBar.add(fileMenu);
        setJMenuBar(menuBar);
    }    

    public DynamoDBBrowser() {
        System.out.println("DynamoDBBrowser constructor called");
        setTitle("DynamoDB Browser");
        setSize(1200, 700);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);
        
        try {
            setIconImage(createAppIcon());
        } catch (Exception e) {
            // Ignore if icon creation fails
        }  
        
        try {
            // Check if settings exist
            String savedArn = prefs.get("tableArn", null);
            String savedProfile = prefs.get("awsProfile", null);
            
            System.out.println("Saved ARN: " + savedArn);
            System.out.println("Saved Profile: " + savedProfile);
            
            // Show the dialog (don't exit immediately if cancelled on first run)
            boolean connected = showConnectionDialog(savedArn, savedProfile);
            System.out.println("Connection result: " + connected);
            setupMenuBar();
            if (!connected) {
                System.out.println("Connection dialog cancelled. Exiting.");
                System.exit(0);
            }
        } catch (Exception e) {
            System.err.println("Error in constructor:");
//            e.printStackTrace();
            JOptionPane.showMessageDialog(this, 
                "Error initializing: " + e.getMessage(),
                "Initialization Error", 
                JOptionPane.ERROR_MESSAGE);
            System.exit(1);
        }
    }
    
    private boolean showConnectionDialog(String defaultArn, String defaultProfile) {
        System.out.println("Showing connection dialog...");
        
        // Read AWS profiles from credentials file
        List<String> profiles = readAwsProfiles();
        
        JPanel panel = new JPanel(new GridLayout(3, 2, 5, 5));
        JTextField arnField = new JTextField(defaultArn != null ? defaultArn : "", 30);
        
        // Use combo box for profiles instead of text field
        JComboBox<String> profileCombo = new JComboBox<>(profiles.toArray(new String[0]));
        if (defaultProfile != null && profiles.contains(defaultProfile)) {
            profileCombo.setSelectedItem(defaultProfile);
        } else if (profiles.contains("default")) {
            profileCombo.setSelectedItem("default");
        }
        profileCombo.setEditable(true); // Allow custom profile names
        
        panel.add(new JLabel("DynamoDB Table ARN:"));
        panel.add(arnField);
        panel.add(new JLabel("AWS Profile:"));
        panel.add(profileCombo);
        
        // Make sure the frame is visible first so the dialog has a parent
        this.setVisible(true);
        this.toFront();
        this.requestFocus();
        
        int result = JOptionPane.showConfirmDialog(this, panel, 
            "Connect to DynamoDB", JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);
        
        System.out.println("Dialog result: " + result);
        
        if (result == JOptionPane.OK_OPTION) {
            tableArn = arnField.getText().trim();
            String profile = (String) profileCombo.getSelectedItem();
            if (profile != null) {
                profile = profile.trim();
            }
            
            if (tableArn.isEmpty() || Objects.requireNonNull(profile).isEmpty()) {
                JOptionPane.showMessageDialog(this, "Please provide both ARN and profile.");
                return showConnectionDialog(tableArn, profile);
            }
            
            // Extract table name and region from ARN
            // ARN format: arn:aws:dynamodb:region:account:table/table-name
            try {
                String[] parts = tableArn.split(":");
                if (parts.length < 6) {
                    throw new IllegalArgumentException("Invalid ARN format");
                }
                String regionStr = parts[3];
                tableName = parts[5].substring(parts[5].indexOf("/") + 1);
                
                // Initialize DynamoDB client
                dynamoDb = DynamoDbClient.builder()
                    .region(Region.of(regionStr))
                    .credentialsProvider(ProfileCredentialsProvider.create(profile))
                    .build();
                
                // Test connection and get table description
                tableDescription = dynamoDb.describeTable(DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build()).table();
                
                // Save settings
                prefs.put("tableArn", tableArn);
                prefs.put("awsProfile", profile);
                
                initializeUI();
                loadInitialRecords();
                return true;
                
            } catch (Exception e) {
                JOptionPane.showMessageDialog(this, 
                    "Error connecting to DynamoDB: " + e.getMessage(),
                    "Connection Error", JOptionPane.ERROR_MESSAGE);
                return showConnectionDialog(tableArn, profile);
            }
        }
        return false;
    }
    
    private List<String> readAwsProfiles() {
        List<String> profiles = new ArrayList<>();
        profiles.add("default"); // Always include default
        
        // Try to read from .aws/credentials file
        String userHome = System.getProperty("user.home");
        File credentialsFile = new File(userHome, ".aws/credentials");
        
        if (credentialsFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(credentialsFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.startsWith("[") && line.endsWith("]")) {
                        String profileName = line.substring(1, line.length() - 1);
                        if (!profiles.contains(profileName)) {
                            profiles.add(profileName);
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading AWS credentials file: " + e.getMessage());
            }
        }
        
        // Also try .aws/config file
        File configFile = new File(userHome, ".aws/config");
        if (configFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(configFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.startsWith("[profile ") && line.endsWith("]")) {
                        String profileName = line.substring(9, line.length() - 1);
                        if (!profiles.contains(profileName)) {
                            profiles.add(profileName);
                        }
                    } else if (line.startsWith("[") && line.endsWith("]") && !line.equals("[default]")) {
                        String profileName = line.substring(1, line.length() - 1);
                        if (!profiles.contains(profileName)) {
                            profiles.add(profileName);
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading AWS config file: " + e.getMessage());
            }
        }
        
        return profiles;
    }
    
    private void initializeUI() {
        Font uiFont = new Font("SansSerif", Font.PLAIN, 14);

        UIManager.put("Label.font", uiFont);
        UIManager.put("Button.font", uiFont);
        UIManager.put("TextField.font", uiFont);
        UIManager.put("PasswordField.font", uiFont);
        UIManager.put("Table.font", uiFont);
        UIManager.put("ProgressBar.font", uiFont);
        UIManager.put("TitledBorder.font", uiFont);
        
        setLayout(new BorderLayout(10, 10));
        
        // Menu bar
        JMenuBar menuBar = new JMenuBar();
        JMenu fileMenu = new JMenu("File");
        JMenuItem exitItem = new JMenuItem("Exit");
        exitItem.addActionListener(e -> {
            int confirm = JOptionPane.showConfirmDialog(this,
                "Are you sure you want to exit?",
                "Confirm Exit",
                JOptionPane.YES_NO_OPTION);
            if (confirm == JOptionPane.YES_OPTION) {
                System.exit(0);
            }
        });
        fileMenu.add(exitItem);
        menuBar.add(fileMenu);
        setJMenuBar(menuBar);
        
        // Top panel with buttons
        JPanel topPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton queryButton;
        queryButton = new JButton("Query Records");
        queryButton.addActionListener(e -> {
            showQueryDialog();
        });
        JButton refreshButton = new JButton("Refresh");
        refreshButton.addActionListener(e -> refreshRecords());
        JButton settingsButton = new JButton("Change Connection");
        settingsButton.addActionListener(e -> {
            if (showConnectionDialog(tableArn, prefs.get("awsProfile", "default"))) {
                refreshRecords();
            }
        });
        
        topPanel.add(new JLabel("Table: " + tableName));
        topPanel.add(queryButton);
        topPanel.add(refreshButton);
        topPanel.add(settingsButton);
        add(topPanel, BorderLayout.NORTH);
        
        // Table for records
        tableModel = new DefaultTableModel() {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };
        recordsTable = new JTable(tableModel);
        recordsTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        recordsTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) {
                    int row = recordsTable.getSelectedRow();
                    if (row >= 0) {
                        showRecordDetails(allRecords.get(row));
                    }
                }
            }
        });
        
        JScrollPane scrollPane = new JScrollPane(recordsTable);
        add(scrollPane, BorderLayout.CENTER);
        
        // Bottom panel with load more button
        JPanel bottomPanel = new JPanel();
        loadMoreButton = new JButton("Load More (50)");
        loadMoreButton.addActionListener(e -> loadMoreRecords());
        loadMoreButton.setEnabled(false);
        bottomPanel.add(loadMoreButton);
        add(bottomPanel, BorderLayout.SOUTH);
    }
    
    private void loadInitialRecords() {
        allRecords.clear();
        lastEvaluatedKey = null;
        tableModel.setRowCount(0);
        tableModel.setColumnCount(0);
        loadMoreRecords();
    }
    
    private void refreshRecords() {
        loadInitialRecords();
    }
    
    private void loadMoreRecords() {
        try {
            ScanRequest.Builder scanBuilder = ScanRequest.builder()
                .tableName(tableName)
                .limit(dynamoQueryLimit);
            
            if (lastEvaluatedKey != null) {
                scanBuilder.exclusiveStartKey(lastEvaluatedKey);
            }
            
            ScanResponse response = dynamoDb.scan(scanBuilder.build());
            List<Map<String, AttributeValue>> items = response.items();
            
            if (items.isEmpty() && allRecords.isEmpty()) {
                JOptionPane.showMessageDialog(this, "No records found in table.");
                return;
            }
            
            // Setup columns on first load
            if (allRecords.isEmpty()) {
                setupTableColumns(items.getFirst());
            }
            
            // Add records
            for (Map<String, AttributeValue> item : items) {
                allRecords.add(item);
                addRecordToTable(item);
            }
            
            lastEvaluatedKey = response.lastEvaluatedKey();
            loadMoreButton.setEnabled(lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
            
        } catch (SdkException e) {
            JOptionPane.showMessageDialog(this, 
                "Error loading records: " + e.getMessage(),
                "Load Error", JOptionPane.ERROR_MESSAGE);
        }
    }
    
    private void setupTableColumns(Map<String, AttributeValue> sampleItem) {
        // Get primary key attributes
        List<String> keyAttributes = new ArrayList<>();
        for (KeySchemaElement key : tableDescription.keySchema()) {
            keyAttributes.add(key.attributeName());
        }
        
        // Add primary key columns first
        for (String keyAttr : keyAttributes) {
            tableModel.addColumn(keyAttr + " (PK)");
        }
        
        // Add a few more columns for context (up to 3 additional)
        int additionalCols = 0;
        for (String attr : sampleItem.keySet()) {
            if (!keyAttributes.contains(attr) && additionalCols < 3) {
                tableModel.addColumn(attr);
                additionalCols++;
            }
        }
    }
    
    private void addRecordToTable(Map<String, AttributeValue> item) {
        Vector<String> row = new Vector<>();
        
        for (int i = 0; i < tableModel.getColumnCount(); i++) {
            String colName = tableModel.getColumnName(i).replace(" (PK)", "");
            AttributeValue value = item.get(colName);
            row.add(formatAttributeValue(value));
        }
        
        tableModel.addRow(row);
    }
    
    private String formatAttributeValue(AttributeValue value) {
        if (value == null) return "";
        if (value.s() != null) return value.s();
        if (value.n() != null) return value.n();
        if (value.bool() != null) return value.bool().toString();
        if (value.nul() != null && value.nul()) return "NULL";
        if (value.ss() != null) return value.ss().toString();
        if (value.ns() != null) return value.ns().toString();
        if (value.m() != null) return "{Map}";
        if (value.l() != null) return "[List]";
        return value.toString();
    }
    
    private void showRecordDetails(Map<String, AttributeValue> record) {
        JDialog dialog = new JDialog(this, "Record Details", true);
        dialog.setSize(700, 600);
        dialog.setLocationRelativeTo(this);
        dialog.setLayout(new BorderLayout());
        
        // Create table model for record details
        DefaultTableModel detailModel = new DefaultTableModel(new String[]{"Attribute", "Value"}, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };
        
        // Sort attributes alphabetically for better readability
        List<String> sortedKeys = new ArrayList<>(record.keySet());
        Collections.sort(sortedKeys);
        
        for (String key : sortedKeys) {
            AttributeValue value = record.get(key);
            detailModel.addRow(new Object[]{key, formatAttributeValueDetailed(value)});
        }
        
        JTable detailTable = new JTable(detailModel);
        detailTable.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        detailTable.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
        
        // Set column widths
        detailTable.getColumnModel().getColumn(0).setPreferredWidth(200);
        detailTable.getColumnModel().getColumn(1).setPreferredWidth(500);
        
        // Enable row height adjustment for wrapped text
        detailTable.setDefaultRenderer(Object.class, new DefaultTableCellRenderer() {
            @Override
            public Component getTableCellRendererComponent(JTable table, Object value,
                    boolean isSelected, boolean hasFocus, int row, int column) {
                Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
                if (c instanceof JLabel label) {
                    String text = value != null ? value.toString() : "";
                    label.setToolTipText(text); // Show full text in tooltip
                }
                return c;
            }
        });
        
        JScrollPane scrollPane = new JScrollPane(detailTable);
        dialog.add(scrollPane, BorderLayout.CENTER);
        
        // Add buttons
        JPanel buttonPanel = new JPanel();
        
        JButton deleteButton = new JButton("Delete Record");
        deleteButton.setForeground(Color.RED);
        deleteButton.addActionListener(e -> {
            int confirm = JOptionPane.showConfirmDialog(dialog,
                "Are you sure you want to delete this record?\nThis action cannot be undone.",
                "Confirm Delete",
                JOptionPane.YES_NO_OPTION,
                JOptionPane.WARNING_MESSAGE);
            
            if (confirm == JOptionPane.YES_OPTION) {
                if (deleteRecord(record)) {
                    JOptionPane.showMessageDialog(dialog, "Record deleted successfully.");
                    dialog.dispose();
                    refreshRecords(); // Refresh the main table
                } else {
                    JOptionPane.showMessageDialog(dialog, 
                        "Failed to delete record. Check console for details.",
                        "Delete Error",
                        JOptionPane.ERROR_MESSAGE);
                }
            }
        });
        
        JButton closeButton = new JButton("Close");
        closeButton.addActionListener(e -> dialog.dispose());
        
        buttonPanel.add(deleteButton);
        buttonPanel.add(closeButton);
        dialog.add(buttonPanel, BorderLayout.SOUTH);
        
        dialog.setVisible(true);
    }
    
    private boolean deleteRecord(Map<String, AttributeValue> record) {
        try {
            // Build the key for deletion from the table's key schema
            Map<String, AttributeValue> key = new HashMap<>();
            for (KeySchemaElement keyElement : tableDescription.keySchema()) {
                String keyName = keyElement.attributeName();
                AttributeValue keyValue = record.get(keyName);
                if (keyValue == null) {
                    System.err.println("Missing key attribute: " + keyName);
                    return false;
                }
                key.put(keyName, keyValue);
            }
            
            DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .build();
            
            dynamoDb.deleteItem(deleteRequest);
            System.out.println("Record deleted successfully: " + key);
            return true;
            
        } catch (SdkException e) {
            System.err.println("Error deleting record: " + e.getMessage());
//            e.printStackTrace();
            return false;
        }
    }
    
    private String formatAttributeValueDetailed(AttributeValue value) {
        if (value == null) return "null";
        if (value.s() != null) return "\"" + value.s() + "\"";
        if (value.n() != null) return value.n();
        if (value.bool() != null) return value.bool().toString();
        if (value.nul() != null && value.nul()) return "NULL";
        if (value.ss() != null) return "String Set: " + value.ss();
        if (value.ns() != null) return "Number Set: " + value.ns();
        if (value.m() != null) return "Map: " + value.m().toString();
        if (value.l() != null) return "List: " + value.l().toString();
        if (value.b() != null) return "Binary: " + value.b().toString();
        return value.toString();
    }
    
    private void showQueryDialog() {
        JDialog dialog = new JDialog(this, "Query Records", true);
        dialog.setSize(600, 500);
        dialog.setLocationRelativeTo(this);
        dialog.setLayout(new BorderLayout(10, 10));
        
        JPanel mainPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(5, 5, 5, 5);
        
        // Index selection with descriptions
        gbc.gridx = 0; gbc.gridy = 0;
        mainPanel.add(new JLabel("Index:"), gbc);
        
        JComboBox<IndexOption> indexCombo = new JComboBox<>();
        
        // Add primary index
        List<KeySchemaElement> primaryKeys = tableDescription.keySchema();
        String primaryDesc = buildIndexDescription("Primary Index", primaryKeys);
        indexCombo.addItem(new IndexOption("Primary Index", primaryDesc, primaryKeys, false));
        
        // Add GSIs with their key schemas
        if (tableDescription.globalSecondaryIndexes() != null) {
            for (GlobalSecondaryIndexDescription gsi : tableDescription.globalSecondaryIndexes()) {
                String gsiDesc = buildIndexDescription(gsi.indexName(), gsi.keySchema());
                indexCombo.addItem(new IndexOption(
                    gsi.indexName(), 
                    gsiDesc, 
                    gsi.keySchema(), 
                    true
                ));
            }
        }
        
        // Custom renderer to show descriptions
        indexCombo.setRenderer(new DefaultListCellRenderer() {
            @Override
            public Component getListCellRendererComponent(JList<?> list, Object value,
                    int index, boolean isSelected, boolean cellHasFocus) {
                super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
                if (value instanceof IndexOption) {
                    setText(((IndexOption) value).description);
                }
                return this;
            }
        });
        
        gbc.gridx = 1; gbc.gridwidth = 2;
        mainPanel.add(indexCombo, gbc);
        
        // Key condition panel
        JPanel keyPanel = new JPanel(new GridBagLayout());
        GridBagConstraints keyGbc = new GridBagConstraints();
        keyGbc.fill = GridBagConstraints.HORIZONTAL;
        keyGbc.insets = new Insets(5, 5, 5, 5);
        
        gbc.gridx = 0; gbc.gridy = 1; gbc.gridwidth = 3;
        mainPanel.add(new JLabel("Key Conditions:"), gbc);
        
        gbc.gridy = 2;
        mainPanel.add(keyPanel, gbc);
        
        // Information label
        JLabel infoLabel = new JLabel("<html><i>Partition key is required. Sort key is optional.</i></html>");
        infoLabel.setForeground(Color.GRAY);
        gbc.gridy = 3;
        mainPanel.add(infoLabel, gbc);
        
        // Update key fields based on index selection
        indexCombo.addActionListener(e -> {
            keyPanel.removeAll();
            IndexOption selected = (IndexOption) indexCombo.getSelectedItem();
            if (selected == null) return;
            
            List<KeySchemaElement> keys = selected.keySchema;
            
            keyGbc.gridx = 0; keyGbc.gridy = 0; keyGbc.gridwidth = 1;
            for (KeySchemaElement key : keys) {
                String keyType = key.keyType() == KeyType.HASH ? " (Partition Key)" : " (Sort Key)";
                keyPanel.add(new JLabel(key.attributeName() + keyType + ":"), keyGbc);
                keyGbc.gridx = 1; keyGbc.gridwidth = 2;
                JTextField field = new JTextField(20);
                field.setName(key.attributeName());
                keyPanel.add(field, keyGbc);
                keyGbc.gridx = 0; keyGbc.gridy++; keyGbc.gridwidth = 1;
            }
            
            keyPanel.revalidate();
            keyPanel.repaint();
        });
        
        // Trigger initial setup
        indexCombo.setSelectedIndex(0);
        
        JScrollPane scrollPane = new JScrollPane(mainPanel);
        scrollPane.setBorder(BorderFactory.createEmptyBorder());
        dialog.add(scrollPane, BorderLayout.CENTER);
        
        // Buttons
        JPanel buttonPanel = new JPanel();
        JButton executeButton = new JButton("Execute Query");
        JButton cancelButton = new JButton("Cancel");
        
        executeButton.addActionListener(e -> {
            IndexOption selected = (IndexOption) indexCombo.getSelectedItem();
            executeQuery(selected, keyPanel);
            dialog.dispose();
        });
        
        cancelButton.addActionListener(e -> dialog.dispose());
        
        buttonPanel.add(executeButton);
        buttonPanel.add(cancelButton);
        dialog.add(buttonPanel, BorderLayout.SOUTH);
        
        dialog.setVisible(true);
    }
    
    private String buildIndexDescription(String indexName, List<KeySchemaElement> keySchema) {
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
    
    // Helper class to store index information
    private static class IndexOption {
        String name;
        String description;
        List<KeySchemaElement> keySchema;
        boolean isGSI;
        
        IndexOption(String name, String description, List<KeySchemaElement> keySchema, boolean isGSI) {
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
    
    private void executeQuery(IndexOption indexOption, JPanel keyPanel) {
        try {
            Map<String, AttributeValue> keyConditions = new HashMap<>();
            
            for (Component comp : keyPanel.getComponents()) {
                if (comp instanceof JTextField field) {
                    String value = field.getText().trim();
                    if (!value.isEmpty()) {
                        keyConditions.put(field.getName(), 
                            AttributeValue.builder().s(value).build());
                    }
                }
            }
            
            if (keyConditions.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Please provide at least the partition key.");
                return;
            }
            
            QueryRequest.Builder queryBuilder = QueryRequest.builder()
                .tableName(tableName)
                .limit(dynamoQueryLimit);
            
            // Build key condition expression
            List<String> conditions = new ArrayList<>();
            Map<String, String> names = new HashMap<>();
            Map<String, AttributeValue> values = new HashMap<>();
            
            int i = 0;
            for (Map.Entry<String, AttributeValue> entry : keyConditions.entrySet()) {
                String placeholder = "#k" + i;
                String valuePlaceholder = ":v" + i;
                names.put(placeholder, entry.getKey());
                values.put(valuePlaceholder, entry.getValue());
                conditions.add(placeholder + " = " + valuePlaceholder);
                i++;
            }
            
            queryBuilder.keyConditionExpression(String.join(" AND ", conditions))
                .expressionAttributeNames(names)
                .expressionAttributeValues(values);
            
            if (indexOption.isGSI) {
                queryBuilder.indexName(indexOption.name);
            }
            
            QueryResponse response = dynamoDb.query(queryBuilder.build());
            
            // Clear and display results
            allRecords.clear();
            tableModel.setRowCount(0);
            lastEvaluatedKey = response.lastEvaluatedKey();
            
            if (response.items().isEmpty()) {
                JOptionPane.showMessageDialog(this, "No records found matching query.");
            } else {
                for (Map<String, AttributeValue> item : response.items()) {
                    allRecords.add(item);
                    addRecordToTable(item);
                }
                loadMoreButton.setEnabled(false); // Query results don't support load more in this simple version
            }
            
        } catch (SdkException e) {
            JOptionPane.showMessageDialog(this, 
                "Query error: " + e.getMessage(),
                "Query Error", JOptionPane.ERROR_MESSAGE);
        }
    }
    
    public static void main(String[] args) {
        System.setProperty("awt.useSystemAAFontSettings", "on");
        System.setProperty("swing.aatext", "true");
        System.out.println("Starting DynamoDB Browser...");

        SwingUtilities.invokeLater(() -> {
            try {
                System.out.println("Initializing UI...");
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
                
                DynamoDBBrowser browser = new DynamoDBBrowser();
                browser.setVisible(true);
                System.out.println("Application started successfully.");
                
            } catch (Exception e) {
                System.err.println("Fatal error starting application:");
//                e.printStackTrace();
                JOptionPane.showMessageDialog(null, 
                    "Error starting application: " + e.getMessage(),
                    "Startup Error", 
                    JOptionPane.ERROR_MESSAGE);
                System.exit(1);
            }
        });
    }
}