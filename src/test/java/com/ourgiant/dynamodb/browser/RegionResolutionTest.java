package com.ourgiant.dynamodb.browser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RegionResolutionTest {

    private DynamoDBBrowser browser;
    private String originalUserHome;

    @TempDir
    Path fakeHome;

    @BeforeEach
    void setUp() {
        browser = ReflectionSupport.newBrowserInstance();
        originalUserHome = System.getProperty("user.home");
        System.setProperty("user.home", fakeHome.toString());
    }

    @AfterEach
    void tearDown() {
        System.setProperty("user.home", originalUserHome);
    }

    private String resolveRegion(String profile) {
        return (String) ReflectionSupport.invoke(browser, "resolveRegionForProfile",
            new Class<?>[]{String.class}, profile);
    }

    @Test
    void usesRegionFromNamedProfileSection() throws IOException {
        writeConfig("""
            [default]
            region = us-east-1

            [profile confA]
            region = ap-southeast-2
            """);

        assertEquals("ap-southeast-2", resolveRegion("confA"));
    }

    @Test
    void usesRegionFromDefaultSection() throws IOException {
        writeConfig("""
            [default]
            region = eu-west-1
            """);

        assertEquals("eu-west-1", resolveRegion("default"));
    }

    @Test
    void fallsBackToDefaultRegionWhenNoConfigOrEnv() {
        // This is only meaningful if the test's own environment doesn't already
        // define AWS_REGION/AWS_DEFAULT_REGION, since the production code checks those too.
        Assumptions.assumeTrue(System.getenv("AWS_REGION") == null);
        Assumptions.assumeTrue(System.getenv("AWS_DEFAULT_REGION") == null);

        assertEquals("us-east-1", resolveRegion("no-such-profile"));
    }

    private void writeConfig(String content) throws IOException {
        Path awsDir = Files.createDirectory(fakeHome.resolve(".aws"));
        Files.writeString(awsDir.resolve("config"), content);
    }
}
