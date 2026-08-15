---
name: verify-java-swing
description: Techniques for verifying a Java Swing/AWT desktop app actually works by launching it and driving the real UI — screenshot limitations in sandboxed/Wayland environments, the modal-dialog invokeAndWait deadlock, synthetic MouseEvent dispatch, and process safety on a shared display. Use whenever debugging or verifying a change to a Swing or AWT GUI app by running it directly (not just unit tests).
---

# Verifying Java Swing/AWT desktop apps

Verifying a Swing app means launching it and driving the real UI, not just
`mvn test`/`gradle test`. This skill covers the toolkit-specific mechanics.
**Check the current repo for its own `.claude/skills/verify/SKILL.md` first**
— that's where project-specific facts belong (build commands, config file
formats, jar paths); this skill is the reusable technique layer underneath it.

## 1. Screenshots may not work — verify before trusting

Try `java.awt.Robot.createScreenCapture(...)` first, but check the output
isn't a solid black image before relying on it. In sandboxed/remote dev
environments running Wayland, this reliably returns solid black for both a
specific window's bounds and the full screen — a compositor-level restriction
on legacy X11 screen capture, not a bug in your capture code or coordinates.

If that happens, **stop trying to fix it** and fall back to non-visual
verification:
- Reflection into live component state (`.getText()` on labels/buttons,
  field values, table cell contents).
- The app's own log output.
- Reading a generated image *file* directly (e.g. an icon asset) — that's
  reading a file, not a screen capture, and works fine.

## 2. Driving the UI: reflection + real event dispatch

Write a small standalone harness (a `.java` file compiled against the app's
jar/classes) rather than trying to interact by hand:

- Construct the app's main window on the EDT:
  `SwingUtilities.invokeAndWait(() -> { win[0] = new MainClass(); win[0].setVisible(true); });`
- Reach private fields/components via reflection:
  `Field f = obj.getClass().getDeclaredField("name"); f.setAccessible(true);`
- Click buttons with `button.doClick()` — fires the real `ActionListener`
  chain, not a bypass.
- For things with no simple click method (right-clicking a `JTable` row to
  trigger a context menu, etc.), dispatch a synthetic `MouseEvent` directly to
  the component instead of relying on `java.awt.Robot` for input delivery —
  Robot-based OS-level clicks were found unreliable for actual delivery in at
  least one sandboxed environment (a separate issue from the screenshot
  restriction above; test independently, don't assume either failure implies
  the other):
  ```java
  Rectangle r = table.getCellRect(row, col, true);   // LOCAL coords, not screen
  int x = r.x + r.width / 2, y = r.y + r.height / 2;
  MouseEvent press = new MouseEvent(table, MouseEvent.MOUSE_PRESSED,
      System.currentTimeMillis(), 0, x, y, 1, /*popupTrigger=*/true, MouseEvent.BUTTON3);
  MouseEvent release = new MouseEvent(table, MouseEvent.MOUSE_RELEASED,
      System.currentTimeMillis(), 0, x, y, 1, false, MouseEvent.BUTTON3);
  table.dispatchEvent(press);
  table.dispatchEvent(release);
  ```
  `dispatchEvent` reaches real registered listeners — this is driving the app
  through its real input handling, not calling internal methods directly.
  Set `popupTrigger=true` on the press event for right-click/context-menu
  triggers (the Linux/GTK convention; some platforms trigger on release
  instead — mirror whatever the app's own listener checks for).

## 3. The modal-dialog `invokeAndWait` deadlock

**Never** do this when the click opens a **modal** dialog
(`JOptionPane.showConfirmDialog`, `showOptionDialog`, a modal `JDialog`, etc.):

```java
SwingUtilities.invokeAndWait(button::doClick);   // DEADLOCKS if this opens a modal dialog
```

The modal dialog pumps its own nested event loop that only returns once
something dismisses it. Nothing can, because the thread that would dismiss it
is the one blocked inside `invokeAndWait`, waiting for the *original* task
(which is stuck showing the dialog) to return. The harness hangs forever with
no error.

Instead, decouple the click from waiting for it:

```java
SwingUtilities.invokeLater(button::doClick);
Thread.sleep(400);                                 // let the dialog open
JDialog dlg = findVisibleDialog();                 // poll Window.getWindows()
// ...read dlg's content via reflection/getText()...
SwingUtilities.invokeAndWait(dlg::dispose);         // fine — separate call, dialog already showing
```

Wrap any harness step that *might* trigger a modal dialog you didn't account
for in a shell `timeout N` as a safety net, so a wrong assumption produces a
clear timeout instead of a silent, indefinite hang.

## 4. Don't trust a short polling window

If you poll for a state change with a timeout and it never arrives, that
means the poll window was too short — not that the operation "took about that
long." A too-short timeout produces a false read that looks like a real (if
slow) completion. Use generous timeouts (tens of seconds, not hundreds of ms,
for anything involving real I/O — network, a browser, a subprocess) with
periodic heartbeat logging in between, so a genuine hang is distinguishable
from a slow-but-real success. If a number keeps coming out suspiciously
identical across runs, suspect it's your own timeout being exhausted, not a
real deadline in the app.

## 5. Process safety on a shared display

If the environment's display is a real, shared desktop session (not an
isolated headless one — check whether other real user processes are already
attached to it before assuming), be careful about process cleanup:

- **Never** clean up a stuck test with a broad process-name match (`pkill -f
  firefox`, `pkill -f chrome`, etc.) — it can kill the user's actual running
  applications sharing that display, not just processes your test launched.
- Track the exact PID of anything you launch yourself (e.g. via
  `ProcessHandle.allProcesses()` filtered to descendants of your own JVM's
  PID, or a PID the app/library itself reports) and kill only that PID.
- Killing your own harness process by its exact, unique class name
  (`pkill -f VerifyThing`) is safe — it can't collide with anything else.
