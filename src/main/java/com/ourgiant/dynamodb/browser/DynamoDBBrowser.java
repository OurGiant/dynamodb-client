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
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.core.exception.SdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBBrowser extends JFrame {
    private static final Logger log = LoggerFactory.getLogger(DynamoDBBrowser.class);
    private final Preferences prefs = Preferences.userNodeForPackage(DynamoDBBrowser.class);
    private DynamoDbClient dynamoDb;
    private String tableName;
    private String tableArn;
    private String connectedProfile;
    private String connectedAccountId;
    private String connectedRegion;
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
  
    public DynamoDBBrowser() {
        log.debug("DynamoDBBrowser constructor called");
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

            log.debug("Saved ARN: {}", savedArn);
            log.debug("Saved profile: {}", savedProfile);

            // Show the dialog (don't exit immediately if cancelled on first run)
            boolean connected = showConnectionDialog(savedArn, savedProfile);
            log.debug("Connection result: {}", connected);
            if (!connected) {
                log.info("Connection dialog cancelled. Exiting.");
                System.exit(0);
            }
        } catch (Exception e) {
            log.error("Error initializing application", e);
            JOptionPane.showMessageDialog(this,
                "Error initializing: " + e.getMessage(),
                "Initialization Error",
                JOptionPane.ERROR_MESSAGE);
            System.exit(1);
        }
    }
    
    private boolean showConnectionDialog(String defaultArn, String defaultProfile) {
        log.debug("Showing connection dialog...");

        // Read AWS profiles from credentials file
        List<String> profiles = readAwsProfiles();

        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.fill = GridBagConstraints.HORIZONTAL;

        // Use combo box for profiles instead of text field
        JComboBox<String> profileCombo = new JComboBox<>(profiles.toArray(new String[0]));
        if (defaultProfile != null && profiles.contains(defaultProfile)) {
            profileCombo.setSelectedItem(defaultProfile);
        } else if (profiles.contains("default")) {
            profileCombo.setSelectedItem("default");
        }
        profileCombo.setEditable(true); // Allow custom profile names

        JLabel profileStatusLabel = new JLabel(" ");

        JComboBox<String> arnCombo = new JComboBox<>();
        arnCombo.setEditable(true); // Allow pasting/typing an ARN directly as a fallback
        if (defaultArn != null && !defaultArn.isEmpty()) {
            arnCombo.addItem(defaultArn);
            arnCombo.setSelectedItem(defaultArn);
        }

        JButton refreshArnsButton = new JButton("Refresh");

        gbc.gridx = 0; gbc.gridy = 0; gbc.weightx = 0;
        panel.add(new JLabel("AWS Profile:"), gbc);
        gbc.gridx = 1; gbc.weightx = 1;
        panel.add(profileCombo, gbc);
        gbc.gridx = 2; gbc.weightx = 0;
        panel.add(profileStatusLabel, gbc);

        gbc.gridx = 0; gbc.gridy = 1; gbc.weightx = 0;
        panel.add(new JLabel("DynamoDB Table ARN:"), gbc);
        gbc.gridx = 1; gbc.weightx = 1;
        panel.add(arnCombo, gbc);
        gbc.gridx = 2; gbc.weightx = 0;
        panel.add(refreshArnsButton, gbc);

        // Table name -> resolved ARN / TableDescription, populated lazily as the user
        // picks entries from the ARN dropdown (see the itemListener below).
        Map<String, String> arnByTableName = new HashMap<>();
        Map<String, TableDescription> descriptionByArn = new HashMap<>();
        // Holds the account ID from the most recent successful profile check, so it can be
        // shown in the window title/delete confirmation without an extra STS call at connect time.
        String[] lastKnownAccountId = new String[1];

        Runnable refreshArns = () -> {
            String profile = comboText(profileCombo);
            if (profile.isEmpty()) {
                return;
            }

            profileStatusLabel.setText("Checking...");
            profileStatusLabel.setForeground(Color.GRAY);
            profileStatusLabel.setToolTipText(null);
            arnCombo.removeAllItems();
            arnCombo.setEnabled(false);
            refreshArnsButton.setEnabled(false);
            arnByTableName.clear();
            descriptionByArn.clear();
            lastKnownAccountId[0] = null;

            new SwingWorker<ProfileActivity, Void>() {
                @Override
                protected ProfileActivity doInBackground() {
                    return checkProfileActivity(profile);
                }

                @Override
                protected void done() {
                    ProfileActivity activity;
                    try {
                        activity = get();
                    } catch (Exception e) {
                        activity = new ProfileActivity(false, null, e.getMessage(), Collections.emptyList(), null);
                    }

                    arnCombo.setEnabled(true);
                    refreshArnsButton.setEnabled(true);

                    if (activity.active) {
                        lastKnownAccountId[0] = activity.accountId;
                        profileStatusLabel.setForeground(new Color(0, 130, 0));
                        profileStatusLabel.setText("● Active");
                        String tooltip = "Account: " + activity.accountId + " (" + activity.region + ")";
                        if (activity.tableNames.isEmpty()) {
                            tooltip += " - no tables found";
                        }
                        profileStatusLabel.setToolTipText(tooltip);
                        for (String name : activity.tableNames) {
                            arnCombo.addItem(name);
                        }
                    } else {
                        profileStatusLabel.setForeground(Color.RED);
                        profileStatusLabel.setText("● Inactive");
                        profileStatusLabel.setToolTipText(activity.errorMessage != null
                            ? activity.errorMessage
                            : "Profile could not be verified");
                    }
                }
            }.execute();
        };

        profileCombo.addActionListener(e -> refreshArns.run());
        refreshArnsButton.addActionListener(e -> refreshArns.run());

        // Resolve the authoritative ARN for a selected table name on demand (DescribeTable
        // per selection), rather than describing every table just to populate the list.
        arnCombo.addItemListener(e -> {
            if (e.getStateChange() != ItemEvent.SELECTED) {
                return;
            }
            if (!(e.getItem() instanceof String tableNameCandidate)) {
                return;
            }
            if (tableNameCandidate.startsWith("arn:") || arnByTableName.containsKey(tableNameCandidate)) {
                return;
            }

            String profile = comboText(profileCombo);
            String region = resolveRegionForProfile(profile);

            new SwingWorker<TableDescription, Void>() {
                @Override
                protected TableDescription doInBackground() {
                    try (DynamoDbClient client = DynamoDbClient.builder()
                            .region(Region.of(region))
                            .credentialsProvider(ProfileCredentialsProvider.create(profile))
                            .build()) {
                        return client.describeTable(DescribeTableRequest.builder()
                            .tableName(tableNameCandidate)
                            .build()).table();
                    }
                }

                @Override
                protected void done() {
                    try {
                        TableDescription description = get();
                        String arn = description.tableArn();
                        arnByTableName.put(tableNameCandidate, arn);
                        descriptionByArn.put(arn, description);
                        if (tableNameCandidate.equals(comboText(arnCombo))) {
                            arnCombo.setSelectedItem(arn);
                        }
                    } catch (Exception ex) {
                        JOptionPane.showMessageDialog(panel,
                            "Could not resolve ARN for table '" + tableNameCandidate + "': " + ex.getMessage(),
                            "ARN Resolution Error", JOptionPane.WARNING_MESSAGE);
                    }
                }
            }.execute();
        });

        // Make sure the frame is visible first so the dialog has a parent
        this.setVisible(true);
        this.toFront();
        this.requestFocus();

        if (defaultProfile != null && !defaultProfile.trim().isEmpty()) {
            refreshArns.run();
        }

        int result = JOptionPane.showConfirmDialog(this, panel,
            "Connect to DynamoDB", JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);

        log.debug("Dialog result: {}", result);

        if (result == JOptionPane.OK_OPTION) {
            String profile = comboText(profileCombo);
            tableArn = comboText(arnCombo);
            if (arnByTableName.containsKey(tableArn)) {
                tableArn = arnByTableName.get(tableArn);
            }

            if (tableArn.isEmpty() || profile.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Please provide both ARN and profile.");
                return showConnectionDialog(tableArn, profile);
            }

            // Extract table name and region from ARN
            try {
                ParsedTableArn parsedArn = parseTableArn(tableArn);
                String regionStr = parsedArn.region();
                tableName = parsedArn.tableName();

                // Initialize DynamoDB client
                dynamoDb = DynamoDbClient.builder()
                    .region(Region.of(regionStr))
                    .credentialsProvider(ProfileCredentialsProvider.create(profile))
                    .build();

                // Reuse the description resolved while populating the dropdown, if available,
                // otherwise test the connection now (e.g. a manually pasted ARN).
                TableDescription cachedDescription = descriptionByArn.get(tableArn);
                tableDescription = cachedDescription != null
                    ? cachedDescription
                    : dynamoDb.describeTable(DescribeTableRequest.builder()
                        .tableName(tableName)
                        .build()).table();

                // Save settings
                prefs.put("tableArn", tableArn);
                prefs.put("awsProfile", profile);

                connectedProfile = profile;
                connectedAccountId = lastKnownAccountId[0];
                connectedRegion = regionStr;
                setTitle(buildWindowTitle());

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

    private String comboText(JComboBox<String> combo) {
        Object editorItem = combo.getEditor().getItem();
        return editorItem != null ? editorItem.toString().trim() : "";
    }

    // Builds the main window title from the current connection, so it's obvious at a glance
    // which profile/account/region/table is connected (e.g. to avoid confusing prod and QA).
    // Degrades gracefully when the account ID isn't known yet (e.g. a manually pasted ARN
    // whose profile's active-check hasn't completed or wasn't run).
    private String buildWindowTitle() {
        StringBuilder title = new StringBuilder("DynamoDB Browser");

        if (connectedProfile != null && !connectedProfile.isEmpty()) {
            title.append(" — ").append(connectedProfile);

            List<String> details = new ArrayList<>();
            if (connectedAccountId != null && !connectedAccountId.isEmpty()) {
                details.add(connectedAccountId);
            }
            if (connectedRegion != null && !connectedRegion.isEmpty()) {
                details.add(connectedRegion);
            }
            if (!details.isEmpty()) {
                title.append(" (").append(String.join(", ", details)).append(")");
            }
        }

        if (tableName != null && !tableName.isEmpty()) {
            title.append(" — ").append(tableName);
        }

        return title.toString();
    }

    // Builds the delete-confirmation message, naming the table/profile/account being affected
    // so the last thing a user sees before confirming names exactly what they're deleting from.
    private String buildDeleteConfirmationMessage() {
        StringBuilder message = new StringBuilder("Are you sure you want to delete this record");

        if (tableName != null && !tableName.isEmpty()) {
            message.append(" from \"").append(tableName).append("\"");
        }

        if (connectedProfile != null && !connectedProfile.isEmpty()) {
            message.append(" (profile: ").append(connectedProfile);
            if (connectedAccountId != null && !connectedAccountId.isEmpty()) {
                message.append(", account: ").append(connectedAccountId);
            }
            message.append(")");
        }

        message.append("?\nThis action cannot be undone.");
        return message.toString();
    }

    // ARN format: arn:aws:dynamodb:region:account:table/table-name
    // Package-private (rather than private) so tests in this package can use the
    // parsed result directly after invoking the private parseTableArn method via reflection.
    record ParsedTableArn(String region, String tableName) {
    }

    private ParsedTableArn parseTableArn(String arn) {
        String[] parts = arn.split(":");
        if (parts.length < 6) {
            throw new IllegalArgumentException("Invalid ARN format");
        }
        String region = parts[3];
        String name = parts[5].substring(parts[5].indexOf("/") + 1);
        return new ParsedTableArn(region, name);
    }

    // Result of checking whether a profile's credentials are currently usable.
    private static class ProfileActivity {
        final boolean active;
        final String accountId;
        final String errorMessage;
        final List<String> tableNames;
        final String region;

        ProfileActivity(boolean active, String accountId, String errorMessage, List<String> tableNames, String region) {
            this.active = active;
            this.accountId = accountId;
            this.errorMessage = errorMessage;
            this.tableNames = tableNames;
            this.region = region;
        }
    }

    // Verifies a profile's credentials via STS and, if active, lists its DynamoDB tables.
    // Runs network calls, so callers must invoke this off the EDT (e.g. from a SwingWorker).
    private ProfileActivity checkProfileActivity(String profile) {
        String region = resolveRegionForProfile(profile);
        try {
            ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create(profile);

            String accountId;
            try (StsClient stsClient = StsClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(credentialsProvider)
                    .build()) {
                GetCallerIdentityResponse identity = stsClient.getCallerIdentity(
                    GetCallerIdentityRequest.builder().build());
                accountId = identity.account();
            }

            List<String> tableNames = new ArrayList<>();
            try (DynamoDbClient client = DynamoDbClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(credentialsProvider)
                    .build()) {
                String startTable = null;
                do {
                    ListTablesResponse response = client.listTables(ListTablesRequest.builder()
                        .exclusiveStartTableName(startTable)
                        .build());
                    tableNames.addAll(response.tableNames());
                    startTable = response.lastEvaluatedTableName();
                } while (startTable != null);
            }

            return new ProfileActivity(true, accountId, null, tableNames, region);
        } catch (Exception e) {
            return new ProfileActivity(false, null, e.getMessage(), Collections.emptyList(), region);
        }
    }

    // Resolves the region to use for a profile: its ~/.aws/config entry, then the
    // standard AWS region environment variables, then a default.
    private String resolveRegionForProfile(String profile) {
        String userHome = System.getProperty("user.home");
        File configFile = new File(userHome, ".aws/config");

        if (configFile.exists()) {
            String sectionHeader = "default".equals(profile) ? "[default]" : "[profile " + profile + "]";
            try (BufferedReader reader = new BufferedReader(new FileReader(configFile))) {
                String line;
                boolean inSection = false;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.startsWith("[") && line.endsWith("]")) {
                        inSection = line.equalsIgnoreCase(sectionHeader);
                        continue;
                    }
                    if (inSection && line.startsWith("region")) {
                        String[] kv = line.split("=", 2);
                        if (kv.length == 2 && !kv[1].trim().isEmpty()) {
                            return kv[1].trim();
                        }
                    }
                }
            } catch (IOException e) {
                log.warn("Error reading AWS config file", e);
            }
        }

        String envRegion = System.getenv("AWS_REGION");
        if (envRegion == null || envRegion.isEmpty()) {
            envRegion = System.getenv("AWS_DEFAULT_REGION");
        }
        return (envRegion != null && !envRegion.isEmpty()) ? envRegion : "us-east-1";
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
                log.warn("Error reading AWS credentials file", e);
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
                log.warn("Error reading AWS config file", e);
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
        fileMenu.setMnemonic(KeyEvent.VK_F);
        JMenuItem exitItem = new JMenuItem("Exit");
        exitItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q,
            Toolkit.getDefaultToolkit().getMenuShortcutKeyMaskEx()));
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
        if (value.hasSs()) return value.ss().toString();
        if (value.hasNs()) return value.ns().toString();
        if (value.hasM()) return "{Map}";
        if (value.hasL()) return "[List]";
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
                buildDeleteConfirmationMessage(),
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
                    log.warn("Missing key attribute: {}", keyName);
                    return false;
                }
                key.put(keyName, keyValue);
            }

            DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .build();

            dynamoDb.deleteItem(deleteRequest);
            log.info("Record deleted successfully: {}", key);
            return true;

        } catch (SdkException e) {
            log.error("Error deleting record", e);
            return false;
        }
    }
    
    private String formatAttributeValueDetailed(AttributeValue value) {
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
                            buildKeyAttributeValue(resolveAttributeType(field.getName()), value));
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
            
        } catch (IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this,
                e.getMessage(),
                "Unsupported Key Type", JOptionPane.WARNING_MESSAGE);
        } catch (SdkException e) {
            JOptionPane.showMessageDialog(this,
                "Query error: " + e.getMessage(),
                "Query Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    // Looks up a key attribute's declared type from the table description, so query key
    // conditions can be built with the correct AttributeValue type (DynamoDB key comparisons
    // are type-strict: an S-typed condition never matches an N-typed key, and vice versa).
    // Defaults to String if the attribute isn't found (shouldn't happen for a real key schema).
    private ScalarAttributeType resolveAttributeType(String attributeName) {
        for (AttributeDefinition definition : tableDescription.attributeDefinitions()) {
            if (definition.attributeName().equals(attributeName)) {
                return definition.attributeType();
            }
        }
        return ScalarAttributeType.S;
    }

    private AttributeValue buildKeyAttributeValue(ScalarAttributeType type, String rawValue) {
        if (type == ScalarAttributeType.N) {
            return AttributeValue.builder().n(rawValue).build();
        }
        if (type == ScalarAttributeType.B) {
            throw new IllegalArgumentException(
                "Binary key attributes aren't supported for querying in this UI.");
        }
        return AttributeValue.builder().s(rawValue).build();
    }

    public static void main(String[] args) {
        System.setProperty("awt.useSystemAAFontSettings", "on");
        System.setProperty("swing.aatext", "true");
        log.info("Starting DynamoDB Browser...");

        SwingUtilities.invokeLater(() -> {
            try {
                log.debug("Initializing UI...");
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());

                DynamoDBBrowser browser = new DynamoDBBrowser();
                browser.setVisible(true);
                log.info("Application started successfully.");

            } catch (Exception e) {
                log.error("Fatal error starting application", e);
                JOptionPane.showMessageDialog(null,
                    "Error starting application: " + e.getMessage(),
                    "Startup Error",
                    JOptionPane.ERROR_MESSAGE);
                System.exit(1);
            }
        });
    }
}