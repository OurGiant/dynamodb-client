package com.ourgiant.dynamodb.browser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AwsProfileParsingTest {

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

    @SuppressWarnings("unchecked")
    private List<String> readProfiles() {
        return (List<String>) ReflectionSupport.invoke(browser, "readAwsProfiles", new Class<?>[]{});
    }

    @Test
    void alwaysIncludesDefaultEvenWithNoFiles() {
        assertEquals(List.of("default"), readProfiles());
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

        assertEquals(List.of("default", "profileA", "confA", "rawSection"), readProfiles());
    }
}
