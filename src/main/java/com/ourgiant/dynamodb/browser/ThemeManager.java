package com.ourgiant.dynamodb.browser;

import com.formdev.flatlaf.FlatDarculaLaf;
import com.formdev.flatlaf.FlatDarkLaf;
import com.formdev.flatlaf.FlatLightLaf;
import com.formdev.flatlaf.extras.FlatAnimatedLafChange;
import com.formdev.flatlaf.intellijthemes.FlatArcDarkOrangeIJTheme;
import com.formdev.flatlaf.intellijthemes.FlatOneDarkIJTheme;
import com.formdev.flatlaf.intellijthemes.FlatSolarizedDarkIJTheme;
import com.formdev.flatlaf.intellijthemes.materialthemeuilite.FlatMTGitHubDarkIJTheme;
import com.formdev.flatlaf.intellijthemes.materialthemeuilite.FlatMTGitHubIJTheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.awt.Window;
import java.util.LinkedHashMap;
import java.util.Map;

/** Theme manager for FlatLaf/IntelliJ themes, ported from doc-scrubber. */
public class ThemeManager {
    private static final Logger logger = LoggerFactory.getLogger(ThemeManager.class);

    private static final Map<String, ThemeInfo> AVAILABLE_THEMES = new LinkedHashMap<>();

    static {
        AVAILABLE_THEMES.put("Flat Light", new ThemeInfo(FlatLightLaf.class.getName(), true));
        AVAILABLE_THEMES.put("GitHub Light", new ThemeInfo(FlatMTGitHubIJTheme.class.getName(), true));
        AVAILABLE_THEMES.put("GitHub Dark", new ThemeInfo(FlatMTGitHubDarkIJTheme.class.getName(), true));
        AVAILABLE_THEMES.put("Flat Dark", new ThemeInfo(FlatDarkLaf.class.getName(), true));
        AVAILABLE_THEMES.put("Darcula", new ThemeInfo(FlatDarculaLaf.class.getName(), true));
        AVAILABLE_THEMES.put("One Dark", new ThemeInfo(FlatOneDarkIJTheme.class.getName(), true));
        AVAILABLE_THEMES.put("Arc Dark Orange", new ThemeInfo(FlatArcDarkOrangeIJTheme.class.getName(), true));
        AVAILABLE_THEMES.put("Solarized Dark", new ThemeInfo(FlatSolarizedDarkIJTheme.class.getName(), true));
    }

    private ThemeManager() {
    }

    public static String[] getAvailableThemeNames() {
        return AVAILABLE_THEMES.keySet().toArray(new String[0]);
    }

    public static boolean applyTheme(String themeName) {
        ThemeInfo themeInfo = AVAILABLE_THEMES.get(themeName);
        if (themeInfo == null) {
            logger.warn("Theme not found: {}", themeName);
            return false;
        }

        boolean animate = themeInfo.isFlatLaf;
        if (animate) {
            FlatAnimatedLafChange.showSnapshot();
        }

        try {
            LookAndFeel currentLaf = UIManager.getLookAndFeel();
            if (currentLaf != null && currentLaf.getName().contains("Nimbus")) {
                flushStaleNimbusDefaults();
            }

            LookAndFeel laf = (LookAndFeel) Class.forName(themeInfo.className)
                .getDeclaredConstructor()
                .newInstance();
            UIManager.setLookAndFeel(laf);

            refreshDisplayableWindows();

            logger.info("Applied theme: {}", themeName);
            return true;
        } catch (Exception e) {
            logger.error("Failed to apply theme: {}", themeName, e);
            return false;
        } finally {
            if (animate) {
                FlatAnimatedLafChange.hideSnapshotWithAnimation();
            }
        }
    }

    private static void flushStaleNimbusDefaults() {
        try {
            UIManager.setLookAndFeel(new FlatDarkLaf());
            refreshDisplayableWindows();
        } catch (Exception e) {
            logger.warn("Failed to flush stale Nimbus defaults via intermediate LaF; proceeding with direct theme switch", e);
        }
    }

    private static void refreshDisplayableWindows() {
        for (Window window : Window.getWindows()) {
            if (!window.isDisplayable()) {
                continue;
            }
            SwingUtilities.updateComponentTreeUI(window);
            window.revalidate();
            window.repaint();
        }
    }

    private static final class ThemeInfo {
        final String className;
        final boolean isFlatLaf;

        ThemeInfo(String className, boolean isFlatLaf) {
            this.className = className;
            this.isFlatLaf = isFlatLaf;
        }
    }
}
