# DynamoDB Browser

[![Build](https://github.com/OurGiant/dynamodb-client/actions/workflows/build.yml/badge.svg)](https://github.com/OurGiant/dynamodb-client/actions/workflows/build.yml)
[![Latest Release](https://img.shields.io/github/v/release/OurGiant/dynamodb-client?label=Release)](https://github.com/OurGiant/dynamodb-client/releases/latest)
[![License: MIT](https://img.shields.io/github/license/OurGiant/dynamodb-client)](LICENSE)
[![Java 24](https://img.shields.io/badge/Java-24-orange?logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Platforms](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20%7C%20Windows-blue)](#build)

A Java Swing desktop application for browsing and querying AWS DynamoDB tables. Connects using local AWS profiles and provides a paginated table view of DynamoDB records.

## Features

- **Active-profile-aware connection dialog**: Select an AWS profile and the table ARN dropdown populates from that profile's own `dynamodb:ListTables` call, with a Refresh button and clear feedback when the profile isn't active
- **Table browsing**: Load and view DynamoDB records in a sortable table, with a stable union of columns across heterogeneous item shapes (single-table-design friendly)
- **Query Records**: Sort-key operators (`=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with`), not just exact match
- **Pagination**: Fetches 50 records at a time with a Load More button, past the initial cap
- **Persistent settings**: Remembers the last-used table ARN and AWS profile between sessions
- **Environment awareness**: Connected AWS account/profile/region shown in the title bar and in the delete confirmation, to avoid running a destructive action against the wrong environment
- **FlatLaf theming**: Switchable Light/Dark/IntelliJ themes via the View menu, persisted between sessions
- **Help > About**: App version, copyright, and a manual/silent (non-blocking) check for newer GitHub releases

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
├── AppPreferences.java     # java.util.prefs wrapper (last table ARN/profile, update-notified version)
├── ThemeManager.java       # FlatLaf theme selection
├── model/                  # Plain data types (ParsedTableArn, IndexOption, ProfileActivity, ...)
├── core/                   # Swing-free domain logic (ARN parsing, query building, AWS profile/region
│                           # resolution, attribute formatting) - no javax.swing.* dependency
├── gui/                    # DynamoDBBrowserFrame, AboutDialog, and all Swing wiring - depends
│                           # one-way on core/model
└── util/                   # Shared helpers with no business meaning of their own
                            # (AppVersion, UpdateChecker, HttpClientFactory, NetworkFetchException)
```

## Dependencies

- **AWS SDK for Java v2**: DynamoDB client, authentication, region resolution
- **FlatLaf** (+ intellij-themes, extras): application theming
- **SLF4J + Logback**: logging
- **Jackson (jackson-databind)**: parsing the GitHub releases API response for the About dialog's update check
- **JUnit 5 + Mockito**: testing

## License

See LICENSE file for details.
