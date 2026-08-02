package com.ourgiant.dynamodb.browser.core;

import com.ourgiant.dynamodb.browser.model.ProfileActivity;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Verifies a profile's credentials via STS and, if active, lists its DynamoDB tables.
// Makes network calls, so callers must invoke this off the EDT (e.g. from a SwingWorker).
public final class AwsProfileActivityChecker {

    private AwsProfileActivityChecker() {
    }

    public static ProfileActivity check(String profile) {
        String region = AwsProfiles.resolveRegionForProfile(profile);
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
}
