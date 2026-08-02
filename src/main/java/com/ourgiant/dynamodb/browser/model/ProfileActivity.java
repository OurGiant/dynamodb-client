package com.ourgiant.dynamodb.browser.model;

import java.util.List;

// Result of checking whether a profile's credentials are currently usable.
public class ProfileActivity {
    public final boolean active;
    public final String accountId;
    public final String errorMessage;
    public final List<String> tableNames;
    public final String region;

    public ProfileActivity(boolean active, String accountId, String errorMessage, List<String> tableNames, String region) {
        this.active = active;
        this.accountId = accountId;
        this.errorMessage = errorMessage;
        this.tableNames = tableNames;
        this.region = region;
    }
}
