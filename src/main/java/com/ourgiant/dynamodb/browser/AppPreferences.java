package com.ourgiant.dynamodb.browser;

import java.util.prefs.Preferences;

/**
 * Local app state: last-connected table ARN/profile, last-notified update version.
 * Backed by {@link Preferences} since this app's own state is small enough that a
 * database would be overkill.
 */
public class AppPreferences {

    private static final String KEY_TABLE_ARN = "tableArn";
    private static final String KEY_AWS_PROFILE = "awsProfile";
    private static final String KEY_LAST_NOTIFIED_UPDATE_VERSION = "lastNotifiedUpdateVersion";

    private final Preferences prefs;

    public AppPreferences() {
        // Preserves the node path that DynamoDBBrowserFrame used directly before this class
        // existed (userNodeForPackage(DynamoDBBrowserFrame.class)), so existing users' saved
        // ARN/profile/update-notified state isn't silently orphaned by this refactor.
        this.prefs = Preferences.userRoot().node("/com/ourgiant/dynamodb/browser/gui");
    }

    public String getTableArn() {
        return prefs.get(KEY_TABLE_ARN, null);
    }

    public void setTableArn(String tableArn) {
        prefs.put(KEY_TABLE_ARN, tableArn);
    }

    public String getAwsProfile(String defaultValue) {
        return prefs.get(KEY_AWS_PROFILE, defaultValue);
    }

    public void setAwsProfile(String awsProfile) {
        prefs.put(KEY_AWS_PROFILE, awsProfile);
    }

    /**
     * The version the silent startup update check last auto-opened the About box for, so it
     * doesn't nag on every single launch while a known update sits unapplied -- once per new
     * version, not once per launch. Null if never notified.
     */
    public String getLastNotifiedUpdateVersion() {
        return prefs.get(KEY_LAST_NOTIFIED_UPDATE_VERSION, null);
    }

    public void setLastNotifiedUpdateVersion(String version) {
        prefs.put(KEY_LAST_NOTIFIED_UPDATE_VERSION, version);
    }
}
