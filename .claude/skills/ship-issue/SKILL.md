---
name: ship-issue
description: The standard workflow for shipping a bug fix or feature to DynamoDB Browser — file a GitHub issue, branch off main, implement, verify, bump the patch version, and open a PR. Use whenever picking up a bug fix or feature for this repo.
---

# Shipping a change to DynamoDB Browser

Follow `java-swing-ship-issue` (the generic workflow shared across the
Java Swing project family) with these DynamoDB Browser specifics:

- **Project path**: `/projects/dynamodb-client` inside the build container.
- **Verify**: use this repo's own `.claude/skills/verify/SKILL.md` for
  build/launch mechanics, and note its "needs a real `~/.aws` profile to
  exercise fully" caveat before claiming a connection/query/delete change
  is verified.
- **This is the one sibling project that talks to real AWS**: a change
  touching `core/AwsProfileActivityChecker`, `core/AwsProfiles`, or
  anything that builds a `DynamoDbClient`/`StsClient` request should be
  double-checked against real AWS behavior (permissions, region
  resolution, pagination) where practical, not just unit-tested against
  mocked SDK responses.
- No repo-specific branch-naming or extra PR-checklist step beyond the
  generic workflow has been established here yet; follow
  `java-swing-ship-issue` as-is until one is.
