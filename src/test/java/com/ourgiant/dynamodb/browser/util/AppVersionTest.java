package com.ourgiant.dynamodb.browser.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;

class AppVersionTest {

    @Test
    void resolveNeverReturnsNullOrBlank() {
        // Running under the test JVM (not the packaged jar), so this exercises the "dev"
        // fallback - there's no Implementation-Version manifest entry and no version.properties
        // on the test classpath (it's Maven-filtered only into the built jar's resources).
        String version = AppVersion.resolve();

        assertNotNull(version);
        assertFalse(version.isBlank());
    }
}
