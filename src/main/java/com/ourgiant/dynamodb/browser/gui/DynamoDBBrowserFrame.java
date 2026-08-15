package com.ourgiant.dynamodb.browser.gui;

import com.ourgiant.dynamodb.browser.AppPreferences;
import com.ourgiant.dynamodb.browser.core.ArnParser;
import com.ourgiant.dynamodb.browser.core.AttributeValueFormatter;
import com.ourgiant.dynamodb.browser.core.AwsProfileActivityChecker;
import com.ourgiant.dynamodb.browser.core.AwsProfiles;
import com.ourgiant.dynamodb.browser.core.ConnectionMessages;
import com.ourgiant.dynamodb.browser.core.IndexDescriptions;
import com.ourgiant.dynamodb.browser.core.KeyConditionBuilder;
import com.ourgiant.dynamodb.browser.core.QueryRequests;
import com.ourgiant.dynamodb.browser.core.RecordGridModel;
import com.ourgiant.dynamodb.browser.ThemeManager;
import com.ourgiant.dynamodb.browser.util.AppVersion;
import com.ourgiant.dynamodb.browser.util.UpdateChecker;
import com.ourgiant.dynamodb.browser.model.IndexOption;
import com.ourgiant.dynamodb.browser.model.KeyConditionBuild;
import com.ourgiant.dynamodb.browser.model.ParsedTableArn;
import com.ourgiant.dynamodb.browser.model.ProfileActivity;
import com.ourgiant.dynamodb.browser.model.SortKeyOperator;

import javax.swing.*;
import javax.swing.table.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.util.List;
import java.net.URL;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.core.exception.SdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBBrowserFrame extends JFrame {
    private static final Logger log = LoggerFactory.getLogger(DynamoDBBrowserFrame.class);
    private final AppPreferences prefs = new AppPreferences();
    private DynamoDbClient dynamoDb;
    private String tableName;
    private String tableArn;
    private String connectedProfile;
    private String connectedAccountId;
    private String connectedRegion;
    private JTable recordsTable;
    private DefaultTableModel tableModel;
    private JButton loadMoreButton;
    private JButton viewDetailsButton;
    private Map<String, AttributeValue> lastEvaluatedKey;
    private final List<Map<String, AttributeValue>> allRecords = new ArrayList<>();
    private TableDescription tableDescription;

    // Set while browsing results from a Query rather than the default table Scan, so
    // "Load More" knows to re-run the same query (with exclusiveStartKey) instead of scanning.
    private boolean queryMode;
    private String activeQueryKeyConditionExpression;
    private Map<String, String> activeQueryExpressionNames;
    private Map<String, AttributeValue> activeQueryExpressionValues;
    private String activeQueryIndexName;
    private final Integer dynamoQueryLimit = (Integer) 50;

    /**
     * Scaled copies of the real app icon (not a hand-drawn placeholder) at the sizes window
     * managers/taskbars actually pick from, via setIconImages -- letting the OS choose the best
     * fit instead of stretching one bitmap.
     */
    private List<Image> loadAppIcons() {
        URL iconUrl = DynamoDBBrowserFrame.class.getResource("/app-icon.png");
        if (iconUrl == null) {
            return List.of();
        }
        Image source = new ImageIcon(iconUrl).getImage();
        List<Image> icons = new ArrayList<>();
        for (int size : new int[]{16, 32, 48, 64, 128}) {
            icons.add(source.getScaledInstance(size, size, Image.SCALE_SMOOTH));
        }
        return icons;
    }

    public DynamoDBBrowserFrame() {
        log.debug("DynamoDBBrowserFrame constructor called");
        setTitle("DynamoDB Browser");
        setSize(1200, 700);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        try {
            setIconImages(loadAppIcons());
        } catch (Exception e) {
            // Ignore if icon loading fails
        }

        try {
            // Check if settings exist
            String savedArn = prefs.getTableArn();
            String savedProfile = prefs.getAwsProfile(null);

            log.debug("Saved ARN: {}", savedArn);
            log.debug("Saved profile: {}", savedProfile);

            // Show the dialog (don't exit immediately if cancelled on first run)
            boolean connected = showConnectionDialog(savedArn, savedProfile);
            log.debug("Connection result: {}", connected);
            if (!connected) {
                log.info("Connection dialog cancelled. Exiting.");
                System.exit(0);
            }
            checkForUpdateAndNotifyIfNewer();
        } catch (Exception e) {
            log.error("Error initializing application", e);
            JOptionPane.showMessageDialog(this,
                "Error initializing: " + e.getMessage(),
                "Initialization Error",
                JOptionPane.ERROR_MESSAGE);
            System.exit(1);
        }
    }

    /**
     * Fires from the constructor, after startup, inside its own SwingWorker so launch is never
     * delayed by network I/O. Dedupes via a "last notified version" preference so the same
     * available version is announced once, not on every launch.
     */
    private void checkForUpdateAndNotifyIfNewer() {
        SwingWorker<Optional<UpdateChecker.ReleaseInfo>, Void> worker = new SwingWorker<>() {
            @Override
            protected Optional<UpdateChecker.ReleaseInfo> doInBackground() {
                return UpdateChecker.fetchLatestRelease();
            }

            @Override
            protected void done() {
                Optional<UpdateChecker.ReleaseInfo> release;
                try {
                    release = get();
                } catch (Exception e) {
                    log.warn("Silent startup update check failed", e);
                    return;
                }
                if (release.isEmpty()) {
                    return;
                }
                UpdateChecker.ReleaseInfo info = release.get();
                if (!UpdateChecker.isNewerVersion(info.version(), AppVersion.resolve())) {
                    return;
                }
                if (info.version().equals(prefs.getLastNotifiedUpdateVersion())) {
                    return;
                }
                prefs.setLastNotifiedUpdateVersion(info.version());
                new AboutDialog(DynamoDBBrowserFrame.this, info).setVisible(true);
            }
        };
        worker.execute();
    }

    private boolean showConnectionDialog(String defaultArn, String defaultProfile) {
        log.debug("Showing connection dialog...");

        // Read AWS profiles from credentials file
        List<String> profiles = AwsProfiles.readAwsProfiles();

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
                    return AwsProfileActivityChecker.check(profile);
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
            String region = AwsProfiles.resolveRegionForProfile(profile);

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
                ParsedTableArn parsedArn = ArnParser.parse(tableArn);
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
                prefs.setTableArn(tableArn);
                prefs.setAwsProfile(profile);

                connectedProfile = profile;
                connectedAccountId = lastKnownAccountId[0];
                connectedRegion = regionStr;
                setTitle(ConnectionMessages.windowTitle(connectedProfile, connectedAccountId, connectedRegion, tableName));

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

        JMenu viewMenu = new JMenu("View");
        JMenu themeMenu = new JMenu("Theme");
        for (String themeName : ThemeManager.getAvailableThemeNames()) {
            JMenuItem item = new JMenuItem(themeName);
            item.addActionListener(e -> ThemeManager.applyTheme(themeName));
            themeMenu.add(item);
        }
        viewMenu.add(themeMenu);
        menuBar.add(viewMenu);

        JMenu helpMenu = new JMenu("Help");
        JMenuItem aboutItem = new JMenuItem("About");
        aboutItem.addActionListener(e -> new AboutDialog(this).setVisible(true));
        helpMenu.add(aboutItem);
        menuBar.add(helpMenu);

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
            if (showConnectionDialog(tableArn, prefs.getAwsProfile("default"))) {
                refreshRecords();
            }
        });

        // Double-click is the original way to open a record, but it depends on the OS/AWT
        // correctly detecting two clicks within its multi-click timing window - over a remote
        // or virtualized desktop session that window can be missed entirely, silently no-opping
        // with no feedback. This button (and the Enter-key binding below) opens the same dialog
        // for whatever row is currently selected, without depending on click timing at all.
        viewDetailsButton = new JButton("View Details");
        viewDetailsButton.setEnabled(false);
        viewDetailsButton.addActionListener(e -> openSelectedRecordDetails());

        topPanel.add(new JLabel("Table: " + tableName));
        topPanel.add(queryButton);
        topPanel.add(refreshButton);
        topPanel.add(viewDetailsButton);
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
        recordsTable.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) {
                viewDetailsButton.setEnabled(recordsTable.getSelectedRow() >= 0);
            }
        });
        recordsTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) {
                    openSelectedRecordDetails();
                }
            }
        });
        recordsTable.getInputMap(JComponent.WHEN_FOCUSED).put(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0), "openRecordDetails");
        recordsTable.getActionMap().put("openRecordDetails", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                openSelectedRecordDetails();
            }
        });

        JScrollPane scrollPane = new JScrollPane(recordsTable);
        add(scrollPane, BorderLayout.CENTER);

        // Bottom panel with load more button
        JPanel bottomPanel = new JPanel();
        loadMoreButton = new JButton("Load More (50)");
        loadMoreButton.addActionListener(e -> {
            if (queryMode) {
                loadMoreQueryResults();
            } else {
                loadMoreRecords();
            }
        });
        loadMoreButton.setEnabled(false);
        bottomPanel.add(loadMoreButton);
        add(bottomPanel, BorderLayout.SOUTH);
    }

    private void loadInitialRecords() {
        allRecords.clear();
        lastEvaluatedKey = null;
        tableModel.setRowCount(0);
        tableModel.setColumnCount(0);
        queryMode = false;
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

            // Set up the primary key column(s) on first load.
            if (allRecords.isEmpty()) {
                setupTableColumns();
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

    private void setupTableColumns() {
        for (String columnName : RecordGridModel.keyColumnNames(tableDescription.keySchema())) {
            tableModel.addColumn(columnName);
        }
    }

    private void addRecordToTable(Map<String, AttributeValue> item) {
        List<String> columnNames = new ArrayList<>();
        for (int i = 0; i < tableModel.getColumnCount(); i++) {
            columnNames.add(tableModel.getColumnName(i));
        }

        tableModel.addRow(new Vector<>(RecordGridModel.formatRow(columnNames, item)));
    }

    private void openSelectedRecordDetails() {
        int row = recordsTable.getSelectedRow();
        if (row >= 0) {
            showRecordDetails(allRecords.get(row));
        }
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
            detailModel.addRow(new Object[]{key, AttributeValueFormatter.formatDetailed(value)});
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
                ConnectionMessages.deleteConfirmationMessage(tableName, connectedProfile, connectedAccountId),
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
        String primaryDesc = IndexDescriptions.buildIndexDescription("Primary Index", primaryKeys);
        indexCombo.addItem(new IndexOption("Primary Index", primaryDesc, primaryKeys, false));

        // Add GSIs with their key schemas
        if (tableDescription.globalSecondaryIndexes() != null) {
            for (GlobalSecondaryIndexDescription gsi : tableDescription.globalSecondaryIndexes()) {
                String gsiDesc = IndexDescriptions.buildIndexDescription(gsi.indexName(), gsi.keySchema());
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
        JLabel infoLabel = new JLabel(
            "<html><i>Partition key is required (exact match). Sort key is optional.</i></html>");
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
                if (key.keyType() == KeyType.HASH) {
                    addPartitionKeyConditionRow(keyPanel, keyGbc, key.attributeName());
                } else {
                    addSortKeyConditionRow(keyPanel, keyGbc, key.attributeName());
                }
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
            // Only close the dialog once we actually have results to show - on a validation
            // error, a failed query, or a query that matched nothing, stay open (so the user
            // can adjust and retry without losing their index/key selections) and leave
            // whatever's already in the grid untouched.
            if (executeQuery(selected, keyPanel)) {
                dialog.dispose();
            }
        });

        cancelButton.addActionListener(e -> dialog.dispose());

        buttonPanel.add(executeButton);
        buttonPanel.add(cancelButton);
        dialog.add(buttonPanel, BorderLayout.SOUTH);

        dialog.setVisible(true);
    }

    // Suffixes distinguishing the sort key's operator combo and "to" (BETWEEN) field from its
    // main value field, which is named after the attribute itself (matching the partition key
    // field's naming, for backward-compatible lookup).
    private static final String SORT_KEY_OPERATOR_SUFFIX = "__operator";
    private static final String SORT_KEY_TO_SUFFIX = "__to";

    private void addPartitionKeyConditionRow(JPanel keyPanel, GridBagConstraints keyGbc, String attributeName) {
        keyPanel.add(new JLabel(attributeName + " (Partition Key):"), keyGbc);
        keyGbc.gridx = 1; keyGbc.gridwidth = 2;
        JTextField field = new JTextField(20);
        field.setName(attributeName);
        keyPanel.add(field, keyGbc);
        keyGbc.gridx = 0; keyGbc.gridy++; keyGbc.gridwidth = 1;

        addSampleValuesHint(keyPanel, keyGbc, attributeName);
    }

    // Adds a sort key condition row to keyPanel: an operator dropdown, one value field, and a
    // second "to" value field that's only enabled when BETWEEN is selected.
    private void addSortKeyConditionRow(JPanel keyPanel, GridBagConstraints keyGbc, String attributeName) {
        keyPanel.add(new JLabel(attributeName + " (Sort Key):"), keyGbc);

        keyGbc.gridx = 1; keyGbc.gridwidth = 1;
        JComboBox<SortKeyOperator> operatorCombo = new JComboBox<>(SortKeyOperator.values());
        operatorCombo.setName(attributeName + SORT_KEY_OPERATOR_SUFFIX);
        keyPanel.add(operatorCombo, keyGbc);

        keyGbc.gridx = 2; keyGbc.gridwidth = 1;
        JTextField valueField = new JTextField(10);
        valueField.setName(attributeName);
        keyPanel.add(valueField, keyGbc);

        keyGbc.gridx = 0; keyGbc.gridy++; keyGbc.gridwidth = 1;
        JLabel toLabel = new JLabel("  and:");
        keyGbc.gridx = 1; keyGbc.gridwidth = 1;
        keyPanel.add(toLabel, keyGbc);

        keyGbc.gridx = 2; keyGbc.gridwidth = 1;
        JTextField toField = new JTextField(10);
        toField.setName(attributeName + SORT_KEY_TO_SUFFIX);
        keyPanel.add(toField, keyGbc);

        Runnable updateToFieldVisibility = () -> {
            boolean isBetween = operatorCombo.getSelectedItem() == SortKeyOperator.BETWEEN;
            toLabel.setVisible(isBetween);
            toField.setVisible(isBetween);
        };
        updateToFieldVisibility.run();
        operatorCombo.addActionListener(e -> {
            updateToFieldVisibility.run();
            keyPanel.revalidate();
            keyPanel.repaint();
        });

        keyGbc.gridx = 0; keyGbc.gridy++; keyGbc.gridwidth = 1;

        addSampleValuesHint(keyPanel, keyGbc, attributeName);
    }

    private void addSampleValuesHint(JPanel keyPanel, GridBagConstraints keyGbc, String attributeName) {
        List<String> samples = RecordGridModel.sampleValues(allRecords, attributeName, 3);
        if (samples.isEmpty()) {
            return;
        }

        JLabel hint = new JLabel("e.g. " + String.join(",  ", samples));
        hint.setForeground(Color.GRAY);
        hint.setFont(hint.getFont().deriveFont(Font.ITALIC, hint.getFont().getSize2D() - 1f));
        keyGbc.gridx = 1; keyGbc.gridwidth = 2;
        keyPanel.add(hint, keyGbc);
        keyGbc.gridx = 0; keyGbc.gridy++; keyGbc.gridwidth = 1;
    }

    // Reads the key condition fields out of keyPanel (built by showQueryDialog's index-selection
    // listener/addSortKeyConditionRow) into plain maps, then hands them to the Swing-free
    // KeyConditionBuilder to actually build the expression.
    private KeyConditionBuild buildKeyConditionExpression(IndexOption indexOption, JPanel keyPanel) {
        Map<String, String> values = new HashMap<>();
        Map<String, SortKeyOperator> operators = new HashMap<>();
        Map<String, String> toValues = new HashMap<>();

        for (KeySchemaElement key : indexOption.keySchema) {
            String attributeName = key.attributeName();
            JTextField valueField = findFieldByName(keyPanel, attributeName);
            values.put(attributeName, valueField != null ? valueField.getText().trim() : "");

            if (key.keyType() != KeyType.HASH) {
                JComboBox<SortKeyOperator> operatorCombo = findOperatorComboByName(keyPanel, attributeName);
                if (operatorCombo != null) {
                    operators.put(attributeName, (SortKeyOperator) operatorCombo.getSelectedItem());
                }
                JTextField toField = findFieldByName(keyPanel, attributeName + SORT_KEY_TO_SUFFIX);
                toValues.put(attributeName, toField != null ? toField.getText().trim() : "");
            }
        }

        return KeyConditionBuilder.build(indexOption.keySchema, tableDescription.attributeDefinitions(),
            values, operators, toValues);
    }

    // Runs the query and, only if it actually returns results, takes over the grid (clearing
    // the current rows and switching into query mode). Returns true in that case (so the
    // caller knows it's safe to close the Query Records dialog), false otherwise - a
    // validation error, a failed query, or one that matched nothing all leave the grid and
    // dialog exactly as they were, so the user can adjust and retry without losing anything.
    private boolean executeQuery(IndexOption indexOption, JPanel keyPanel) {
        KeyConditionBuild build;
        try {
            build = buildKeyConditionExpression(indexOption, keyPanel);
        } catch (IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Invalid Query", JOptionPane.WARNING_MESSAGE);
            return false;
        }

        String indexName = indexOption.isGSI ? indexOption.name : null;
        QueryResponse response;
        try {
            response = dynamoDb.query(QueryRequests.build(tableName, dynamoQueryLimit, build.expression(),
                build.names(), build.values(), indexName, null));
        } catch (SdkException e) {
            JOptionPane.showMessageDialog(this,
                "Query error: " + e.getMessage(),
                "Query Error", JOptionPane.ERROR_MESSAGE);
            return false;
        }

        if (response.items().isEmpty()) {
            JOptionPane.showMessageDialog(this, "No records found matching query.");
            return false;
        }

        activeQueryKeyConditionExpression = build.expression();
        activeQueryExpressionNames = build.names();
        activeQueryExpressionValues = build.values();
        activeQueryIndexName = indexName;
        queryMode = true;

        allRecords.clear();
        tableModel.setRowCount(0);
        for (Map<String, AttributeValue> item : response.items()) {
            allRecords.add(item);
            addRecordToTable(item);
        }

        lastEvaluatedKey = response.lastEvaluatedKey();
        loadMoreButton.setEnabled(lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
        return true;
    }

    private JTextField findFieldByName(JPanel panel, String name) {
        for (Component comp : panel.getComponents()) {
            if (comp instanceof JTextField field && name.equals(field.getName())) {
                return field;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private JComboBox<SortKeyOperator> findOperatorComboByName(JPanel panel, String attributeName) {
        String targetName = attributeName + SORT_KEY_OPERATOR_SUFFIX;
        for (Component comp : panel.getComponents()) {
            if (comp instanceof JComboBox<?> combo && targetName.equals(combo.getName())) {
                return (JComboBox<SortKeyOperator>) combo;
            }
        }
        return null;
    }

    // Fetches the next page for the query already active in the grid (see executeQuery),
    // the same way loadMoreRecords does for a Scan.
    private void loadMoreQueryResults() {
        try {
            QueryRequest request = QueryRequests.build(tableName, dynamoQueryLimit, activeQueryKeyConditionExpression,
                activeQueryExpressionNames, activeQueryExpressionValues, activeQueryIndexName, lastEvaluatedKey);
            QueryResponse response = dynamoDb.query(request);

            for (Map<String, AttributeValue> item : response.items()) {
                allRecords.add(item);
                addRecordToTable(item);
            }

            lastEvaluatedKey = response.lastEvaluatedKey();
            loadMoreButton.setEnabled(lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

        } catch (SdkException e) {
            JOptionPane.showMessageDialog(this,
                "Error loading more records: " + e.getMessage(),
                "Load Error", JOptionPane.ERROR_MESSAGE);
        }
    }
}
