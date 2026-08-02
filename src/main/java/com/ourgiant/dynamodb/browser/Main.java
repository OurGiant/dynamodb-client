package com.ourgiant.dynamodb.browser;

import com.ourgiant.dynamodb.browser.gui.DynamoDBBrowserFrame;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        System.setProperty("awt.useSystemAAFontSettings", "on");
        System.setProperty("swing.aatext", "true");
        log.info("Starting DynamoDB Browser...");

        SwingUtilities.invokeLater(() -> {
            try {
                log.debug("Initializing UI...");
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());

                DynamoDBBrowserFrame browser = new DynamoDBBrowserFrame();
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
