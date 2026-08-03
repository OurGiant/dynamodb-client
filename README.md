# DynamoDB Browser

A Java Swing desktop application for browsing and querying AWS DynamoDB tables. Connects using local AWS profiles and provides a paginated table view of DynamoDB records.

## Features

- **Profile-based authentication**: Connects using any named AWS credential profile from `~/.aws/credentials`
- **Table browsing**: Load and view DynamoDB records in a sortable table
- **Pagination**: Fetches 50 records at a time with a Load More button
- **Persistent settings**: Remembers the last-used table ARN and AWS profile between sessions
- **Dynamic columns**: Table columns are derived from the returned item attributes

## Prerequisites

- Java 24 or higher
- AWS credentials configured at `~/.aws/credentials`
- Network access to AWS DynamoDB

## IAM Permissions

The connected AWS profile needs at least the following actions:

- `dynamodb:DescribeTable` — resolving a table's key schema/indexes on connect and when populating the ARN dropdown
- `dynamodb:Scan` — the default table browsing view
- `dynamodb:Query` — the Query Records dialog
- `dynamodb:DeleteItem` — deleting a record from the Record Details dialog
- `dynamodb:ListTables` — populating the ARN dropdown with the profile's tables
- `sts:GetCallerIdentity` — verifying a profile's credentials are active and showing its account ID in the window title

## Build

```bash
mvn clean package
```

Produces `target/dynamodb-browser-all.jar`.

## Run

```bash
java -jar target/dynamodb-browser-all.jar
```

On launch, a connection dialog prompts for the DynamoDB table ARN and AWS profile name. These values are saved for subsequent runs.

## Project Structure

```
src/main/java/com/ourgiant/dynamodb/browser/
├── Main.java               # Entry point
├── ThemeManager.java       # FlatLaf theme selection
├── model/                  # Plain data types (ParsedTableArn, IndexOption, ProfileActivity, ...)
├── core/                   # Swing-free domain logic (ARN parsing, query building, AWS profile/region
│                           # resolution, attribute formatting) - no javax.swing.* dependency
├── gui/                    # DynamoDBBrowserFrame and all Swing wiring, depends one-way on core/model
└── util/                   # Shared helpers with no business meaning of their own (AppVersion)
```

## Dependencies

- **AWS SDK for Java v2**: DynamoDB client, authentication, region resolution
- **FlatLaf** (+ intellij-themes, extras): application theming
- **SLF4J + Logback**: logging
- **JUnit 5 + Mockito**: testing

## License

See LICENSE file for details.
