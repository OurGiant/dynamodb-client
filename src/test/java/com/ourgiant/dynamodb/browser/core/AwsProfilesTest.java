package com.ourgiant.dynamodb.browser.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AwsProfilesTest {

    private String originalUserHome;

    @TempDir
    Path fakeHome;

    @BeforeEach
    void setUp() {
        originalUserHome = System.getProperty("user.home");
        System.setProperty("user.home", fakeHome.toString());
    }

    @AfterEach
    void tearDown() {
        System.setProperty("user.home", originalUserHome);
    }

    @Test
    void alwaysIncludesDefaultEvenWithNoFiles() {
        assertEquals(List.of("default"), AwsProfiles.readAwsProfiles());
    }

    @Test
    void mergesProfilesFromCredentialsAndConfigWithoutDuplicates() throws IOException {
        Path awsDir = Files.createDirectory(fakeHome.resolve(".aws"));

        Files.writeString(awsDir.resolve("credentials"), """
            [default]
            aws_access_key_id = x
            aws_secret_access_key = y

            [profileA]
            aws_access_key_id = a
            aws_secret_access_key = b
            """);

        Files.writeString(awsDir.resolve("config"), """
            [default]
            region = us-east-1

            [profile confA]
            region = us-east-1

            [rawSection]
            some_key = val
            """);

        assertEquals(List.of("default", "profileA", "confA", "rawSection"), AwsProfiles.readAwsProfiles());
    }

    @Test
    void usesRegionFromNamedProfileSection() throws IOException {
        writeConfig("""
            [default]
            region = us-east-1

            [profile confA]
            region = ap-southeast-2
            """);

        assertEquals("ap-southeast-2", AwsProfiles.resolveRegionForProfile("confA"));
    }

    @Test
    void usesRegionFromDefaultSection() throws IOException {
        writeConfig("""
            [default]
            region = eu-west-1
            """);

        assertEquals("eu-west-1", AwsProfiles.resolveRegionForProfile("default"));
    }

    @Test
    void fallsBackToDefaultRegionWhenNoConfigOrEnv() {
        // This is only meaningful if the test's own environment doesn't already
        // define AWS_REGION/AWS_DEFAULT_REGION, since the production code checks those too.
        Assumptions.assumeTrue(System.getenv("AWS_REGION") == null);
        Assumptions.assumeTrue(System.getenv("AWS_DEFAULT_REGION") == null);

        assertEquals("us-east-1", AwsProfiles.resolveRegionForProfile("no-such-profile"));
    }

    private void writeConfig(String content) throws IOException {
        Path awsDir = Files.createDirectory(fakeHome.resolve(".aws"));
        Files.writeString(awsDir.resolve("config"), content);
    }
}
