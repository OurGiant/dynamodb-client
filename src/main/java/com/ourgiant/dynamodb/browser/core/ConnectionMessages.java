package com.ourgiant.dynamodb.browser.core;

import java.util.ArrayList;
import java.util.List;

public final class ConnectionMessages {

    private ConnectionMessages() {
    }

    // Builds the main window title from the current connection, so it's obvious at a glance
    // which profile/account/region/table is connected (e.g. to avoid confusing prod and QA).
    // Degrades gracefully when the account ID isn't known yet (e.g. a manually pasted ARN
    // whose profile's active-check hasn't completed or wasn't run).
    public static String windowTitle(String connectedProfile, String connectedAccountId, String connectedRegion,
            String tableName) {
        StringBuilder title = new StringBuilder("DynamoDB Browser");

        if (connectedProfile != null && !connectedProfile.isEmpty()) {
            title.append(" — ").append(connectedProfile);

            List<String> details = new ArrayList<>();
            if (connectedAccountId != null && !connectedAccountId.isEmpty()) {
                details.add(connectedAccountId);
            }
            if (connectedRegion != null && !connectedRegion.isEmpty()) {
                details.add(connectedRegion);
            }
            if (!details.isEmpty()) {
                title.append(" (").append(String.join(", ", details)).append(")");
            }
        }

        if (tableName != null && !tableName.isEmpty()) {
            title.append(" — ").append(tableName);
        }

        return title.toString();
    }

    // Builds the delete-confirmation message, naming the table/profile/account being affected
    // so the last thing a user sees before confirming names exactly what they're deleting from.
    public static String deleteConfirmationMessage(String tableName, String connectedProfile,
            String connectedAccountId) {
        StringBuilder message = new StringBuilder("Are you sure you want to delete this record");

        if (tableName != null && !tableName.isEmpty()) {
            message.append(" from \"").append(tableName).append("\"");
        }

        if (connectedProfile != null && !connectedProfile.isEmpty()) {
            message.append(" (profile: ").append(connectedProfile);
            if (connectedAccountId != null && !connectedAccountId.isEmpty()) {
                message.append(", account: ").append(connectedAccountId);
            }
            message.append(")");
        }

        message.append("?\nThis action cannot be undone.");
        return message.toString();
    }
}
