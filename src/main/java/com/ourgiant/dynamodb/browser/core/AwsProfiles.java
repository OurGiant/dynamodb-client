package com.ourgiant.dynamodb.browser.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Reads AWS profile names and per-profile region from the AWS config directory
// (normally ~/.aws, overridable - see configDir()).
public final class AwsProfiles {

    private static final Logger log = LoggerFactory.getLogger(AwsProfiles.class);

    // Overridable via a surefire-set system property (see pom.xml's <systemPropertyVariables>),
    // so tests never touch the real developer's ~/.aws/credentials or ~/.aws/config - mirrors
    // kiro-control-panel's kiro.control.panel.changeLogFile pattern: a scoped override the
    // production code itself understands, rather than tests reaching in and swapping the
    // global user.home system property.
    private static final String CONFIG_DIR_OVERRIDE_PROPERTY = "dynamodb.browser.awsConfigDir";

    private AwsProfiles() {
    }

    private static File configDir() {
        String override = System.getProperty(CONFIG_DIR_OVERRIDE_PROPERTY);
        if (override != null && !override.isBlank()) {
            return new File(override);
        }
        return new File(System.getProperty("user.home"), ".aws");
    }

    public static List<String> readAwsProfiles() {
        List<String> profiles = new ArrayList<>();
        profiles.add("default"); // Always include default

        File configDir = configDir();

        // Try to read from .aws/credentials file
        File credentialsFile = new File(configDir, "credentials");

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
        File configFile = new File(configDir, "config");
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

    // Resolves the region to use for a profile: its .aws/config entry, then the
    // standard AWS region environment variables, then a default.
    public static String resolveRegionForProfile(String profile) {
        File configFile = new File(configDir(), "config");

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
}
