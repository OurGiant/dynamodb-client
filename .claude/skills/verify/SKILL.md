---
name: verify
description: How to build, launch, and drive the DynamoDB Browser to verify a Swing UI change actually works on this dev setup. Use before trusting the generic verify-java-swing skill's screenshot step; read that skill too for the underlying techniques (modal-dialog deadlock, synthetic MouseEvent dispatch, process safety).
---

# Verifying DynamoDB Browser

This is the project-specific companion to the generic `verify-java-swing`
skill (techniques) and `java-swing-project-setup` (build/structure
standard this project follows). Read those first — this file is what to
actually type for *this* project.

## Build here, run there

Maven only exists in the Docker container, not on the host:

```bash
docker exec festive_bardeen bash -c "cd /projects/dynamodb-client && mvn -q package -DskipTests"
```

If `festive_bardeen` doesn't respond, find the current container:
`docker ps -a --format '{{.Names}} {{.Status}} {{.Image}}'` and
`docker start <name>` if stopped — the name can drift across sessions.

`/projects` is bind-mounted from the host's `~/projects`, so the jar lands
at `target/dynamodb-browser-all.jar`, visible on the host. The container is
headless (no `DISPLAY`) — run the jar on the **host**, not inside the
container, or it dies at `JFrame` construction with `HeadlessException`.

```bash
java -jar target/dynamodb-browser-all.jar
```

Main class: `com.ourgiant.dynamodb.browser.Main`.

## This app needs a real ~/.aws profile to exercise fully

Unlike the other sibling projects, DynamoDB Browser talks to real AWS
(`dynamodb:Scan`/`Query`/`DescribeTable`, `sts:GetCallerIdentity`) — there's
no mock/offline mode. The connection dialog (profile list, ARN dropdown) can
be verified without real credentials, since `AwsProfiles.readAwsProfiles()`
just parses whatever `~/.aws/credentials`/`config` exist on the dev host. But
actually connecting to a table, browsing records, querying, and deleting all
require a working profile with real DynamoDB permissions — don't assume
those code paths are exercised just because the connection dialog renders
correctly.

## The connection dialog is modal — construct the frame with `invokeLater`, not `invokeAndWait`

`new DynamoDBBrowserFrame()` shows a modal `JOptionPane` ("Connect to
DynamoDB") synchronously inside its constructor, before the constructor
returns. Constructing it via `SwingUtilities.invokeAndWait(...)` deadlocks
for exactly the reason `verify-java-swing` §3 describes. Use `invokeLater`,
then poll `Window.getWindows()` for a visible `JDialog` titled "Connect to
DynamoDB" instead:

```java
SwingUtilities.invokeLater(() -> new DynamoDBBrowserFrame());
// then poll Window.getWindows() for the visible "Connect to DynamoDB" JDialog
```

The main window's own menu bar (File/View/Help) isn't built until
`initializeUI()` runs, which only happens after a *successful* connection —
so verifying menu items (Theme switching, About) requires actually
connecting to a table, not just reaching the connection dialog.

## Screenshots: Robot works here — confirmed for this project directly

`Robot.createScreenCapture(...)` returns a genuine, non-black screenshot on
this dev host (`DISPLAY=:1`, real X11 session) — confirmed by sampling pixel
values (non-zero average RGB), not just eyeballing the PNG. This matches
`doc-scrubber`'s finding on the same kind of host, but was re-verified
independently for this project rather than assumed, per `java-swing-project-setup`
§6's guidance not to carry a sibling's gotcha over without checking.

## Nothing else confirmed yet

No other project-specific gotchas (first-run state location quirks, custom
dialog sizing issues, etc.) have been found and confirmed here yet. Add them
to this file as they turn up, the way `kiro-control-panel`'s `verify` skill
records its `JEditorPane` sizing gotcha.
