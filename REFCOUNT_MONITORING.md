# RPyC Refcount Error Monitoring

## What changed

Now **all critical refcount errors are ALWAYS logged to stderr**, regardless of the configuration.

### Which messages are logged

1. **On connection initialization:**
   ```
   INFO: RPyC Connection <conn_id> initialized. Refcount error monitoring: ENABLED (errors always logged to stderr)
   ```

2. **On a DECREF for a missing key:**
   ```
   WARNING: [REFCOUNT] DECREF on missing key <id_pack>
   ```

3. **On failure to delete a remote object:**
   ```
   WARNING: Failed to delete remote object <id_pack>. Possible memory leak on remote side.
   ```

4. **On an exception during deletion:**
   ```
   ERROR: Error deleting remote object <id_pack>: <exception>
   <traceback>
   ```

## Where to look for these messages

### Application logs

These messages are written to the process's stderr. If your application redirects
stderr to a file, look in that log file (referred to below as `<stderr-log>`).

### What to check

#### 1. Verify that monitoring is active

After **starting the server** or **a client connecting**, the following should appear:
```bash
grep "INFO:.*Refcount error monitoring: ENABLED" <stderr-log>
```

**Expected result:**
```
INFO: RPyC Connection conn1 initialized. Refcount error monitoring: ENABLED (errors always logged to stderr)
INFO: RPyC Connection conn2 initialized. Refcount error monitoring: ENABLED (errors always logged to stderr)
```

If there are **NO** such messages, it means:
- The server has not been started yet
- There have been no new connections since updating rpyc_async
- Stderr is not redirected to a file

#### 2. Check for refcount errors

**Search for all WARNING and ERROR messages:**
```bash
grep -E "WARNING:|ERROR:" <stderr-log>
```

**Search for specific refcount errors:**
```bash
grep -E "REFCOUNT|Failed to delete" <stderr-log>
```

**If there are errors**, the output will look roughly like this:
```
WARNING: [REFCOUNT] DECREF on missing key ('builtins.method', 10665440, 123474125771264)
WARNING: Failed to delete remote object ('builtins.dict', 10733632, 139475418934272). Possible memory leak on remote side.
```

**If there are NO errors** - that's good! It means refcount is working correctly.

#### 3. Count the number of errors

**Count DECREF errors:**
```bash
grep -c "DECREF on missing key" <stderr-log>
```

**Count Failed to delete:**
```bash
grep -c "Failed to delete remote object" <stderr-log>
```

**If the counter is > 0**, there are refcount problems.

### Example of a full check

```bash
#!/bin/bash

LOG_FILE=<stderr-log>

echo "=== Checking refcount monitoring ==="
echo

# 1. Verify that monitoring is active
echo "1. Checking monitoring activation:"
if grep -q "Refcount error monitoring: ENABLED" "$LOG_FILE" 2>/dev/null; then
    echo "   ✅ Monitoring is ACTIVE"
    count=$(grep -c "Refcount error monitoring: ENABLED" "$LOG_FILE")
    echo "   📊 Connections initialized: $count"
else
    echo "   ⚠️  No activation messages found"
    echo "   (the server may not have been restarted after the update)"
fi

echo

# 2. Count the errors
echo "2. Error statistics:"
decref_count=$(grep -c "DECREF on missing key" "$LOG_FILE" 2>/dev/null || echo "0")
failed_count=$(grep -c "Failed to delete remote object" "$LOG_FILE" 2>/dev/null || echo "0")

echo "   [REFCOUNT] DECREF on missing key: $decref_count"
echo "   Failed to delete remote object: $failed_count"

if [ "$decref_count" -gt 0 ] || [ "$failed_count" -gt 0 ]; then
    echo "   ❌ THERE ARE ERRORS! Investigation required."
else
    echo "   ✅ No errors"
fi

echo

# 3. Latest errors (if any)
total_errors=$((decref_count + failed_count))
if [ "$total_errors" -gt 0 ]; then
    echo "3. Last 5 errors:"
    grep -E "REFCOUNT|Failed to delete" "$LOG_FILE" | tail -5
fi

echo
echo "=== Check complete ==="
```

**Save it to a file** `check_refcount.sh` and run it:
```bash
chmod +x check_refcount.sh
./check_refcount.sh
```

## Interpreting the results

### Normal operation

```
=== Checking refcount monitoring ===

1. Checking monitoring activation:
   ✅ Monitoring is ACTIVE
   📊 Connections initialized: 15

2. Error statistics:
   [REFCOUNT] DECREF on missing key: 0
   Failed to delete remote object: 0
   ✅ No errors

=== Check complete ===
```

**This is good!** The system is working correctly.

### Errors detected

```
=== Checking refcount monitoring ===

1. Checking monitoring activation:
   ✅ Monitoring is ACTIVE
   📊 Connections initialized: 8

2. Error statistics:
   [REFCOUNT] DECREF on missing key: 234
   Failed to delete remote object: 12
   ❌ THERE ARE ERRORS! Investigation required.

3. Last 5 errors:
   WARNING: [REFCOUNT] DECREF on missing key ('builtins.method', 10665440, 123474125771264)
   WARNING: Failed to delete remote object ('builtins.dict', 10733632, 139475418934272)
   ...

=== Check complete ===
```

**This is a problem!** You need to:
1. Save the full log for analysis
2. Check when the errors started
3. Correlate with events (large requests, long-running operation, etc.)

## What to do when errors are detected

### 1. Save the logs

```bash
cp <stderr-log> ~/refcount_errors_$(date +%Y%m%d_%H%M%S).log
```

### 2. Analyze the patterns

**Which objects appear most often?**
```bash
grep "DECREF on missing key" ~/refcount_errors_*.log | \
  sed "s/.*('\([^']*\)'.*/\1/" | sort | uniq -c | sort -rn
```

**At what time do they occur?**
```bash
grep -E "REFCOUNT|Failed to delete" ~/refcount_errors_*.log | head -20
```

### 3. Correlate with load

- Check the log file size: `ls -lh <stderr-log>`
- If the log is huge → many errors → high load
- If the log is small → rare errors → a specific bug

### 4. Reporting

Create an issue with the following information:
- The number of errors
- The object types (bound method, dict, list, etc.)
- The time period
- The conditions under which they occur (if known)
- Attach log excerpts

## Disabling monitoring (NOT RECOMMENDED)

If for some reason you need to disable the error monitoring in stderr, you can redirect stderr to /dev/null when starting the application:

```bash
your-application 2>/dev/null
```

**BUT THIS IS A BAD IDEA!** You will lose important diagnostic information.

## Bottom line

**Now you will ALWAYS see refcount errors** in your application's stderr log.

**Check after the next server start:**
```bash
# 1. Start/restart the server

# 2. Wait a bit for initialization
sleep 2

# 3. Verify that monitoring is active
tail -20 <stderr-log> | grep "INFO:"

# Expected output:
# INFO: RPyC Connection conn1 initialized. Refcount error monitoring: ENABLED (errors always logged to stderr)
```

**If you see this message** - everything works! 🎉

**If there are no refcount errors** - great! The system is working correctly! ✅

**If there are errors** - now you can see them and investigate! 🔍
