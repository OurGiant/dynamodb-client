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
└── DynamoDBBrowser.java    # Main application window and DynamoDB client
```

## Dependencies

- **AWS SDK for Java v2**: DynamoDB client, authentication, region resolution

## License

See LICENSE file for details.
