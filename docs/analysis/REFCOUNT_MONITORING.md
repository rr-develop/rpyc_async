# RPyC Refcount Error Monitoring

## What Changed

Now **all critical refcount errors are ALWAYS logged to stderr**, regardless of the configuration.

### Which Messages Are Logged

1. **On connection initialization:**
   ```
   INFO: RPyC Connection <conn_id> initialized. Refcount error monitoring: ENABLED (errors always logged to stderr)
   ```

2. **On a DECREF error for a missing key:**
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

## Where to Find These Messages

### Application stderr logs

These messages are written to the stderr stream of whichever process hosts the RPyC connection. If your host process redirects stderr to a file, look for them there (referred to below as the stderr log file).

### What to Check

#### 1. Verify That Monitoring Is Active

After **starting the process** or **a client connecting**, the following should appear:
```bash
grep "INFO:.*Refcount error monitoring: ENABLED" <stderr-log>
```

**Expected result:**
```
INFO: RPyC Connection conn1 initialized. Refcount error monitoring: ENABLED (errors always logged to stderr)
INFO: RPyC Connection conn2 initialized. Refcount error monitoring: ENABLED (errors always logged to stderr)
```

If there are **NO** such messages, it means:
- The process is not started yet
- There have been no new connections since the update
- stderr is not redirected to a file

#### 2. Check for Refcount Errors

**Search for all WARNING and ERROR entries:**
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

**If there are NO errors** - that is good! It means refcount works correctly.

#### 3. Count the Number of Errors

**Count DECREF errors:**
```bash
grep -c "DECREF on missing key" <stderr-log>
```

**Count Failed to delete:**
```bash
grep -c "Failed to delete remote object" <stderr-log>
```

**If the count is > 0**, there are refcount problems.

### Full Check Example

```bash
#!/bin/bash

LOG_FILE=<stderr-log>

echo "=== Refcount monitoring check ==="
echo

# 1. Verify that monitoring is active
echo "1. Monitoring activation check:"
if grep -q "Refcount error monitoring: ENABLED" "$LOG_FILE" 2>/dev/null; then
    echo "   ✅ Monitoring is ACTIVE"
    count=$(grep -c "Refcount error monitoring: ENABLED" "$LOG_FILE")
    echo "   📊 Connections initialized: $count"
else
    echo "   ⚠️  No activation messages found"
    echo "   (the process may not have been restarted after the update)"
fi

echo

# 2. Count errors
echo "2. Error statistics:"
decref_count=$(grep -c "DECREF on missing key" "$LOG_FILE" 2>/dev/null || echo "0")
failed_count=$(grep -c "Failed to delete remote object" "$LOG_FILE" 2>/dev/null || echo "0")

echo "   [REFCOUNT] DECREF on missing key: $decref_count"
echo "   Failed to delete remote object: $failed_count"

if [ "$decref_count" -gt 0 ] || [ "$failed_count" -gt 0 ]; then
    echo "   ❌ ERRORS PRESENT! Investigation required."
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

**Save it to a file** `check_refcount.sh` and run:
```bash
chmod +x check_refcount.sh
./check_refcount.sh
```

## Interpreting the Results

### Normal Operation

```
=== Refcount monitoring check ===

1. Monitoring activation check:
   ✅ Monitoring is ACTIVE
   📊 Connections initialized: 15

2. Error statistics:
   [REFCOUNT] DECREF on missing key: 0
   Failed to delete remote object: 0
   ✅ No errors

=== Check complete ===
```

**This is good!** The system works correctly.

### Errors Detected

```
=== Refcount monitoring check ===

1. Monitoring activation check:
   ✅ Monitoring is ACTIVE
   📊 Connections initialized: 8

2. Error statistics:
   [REFCOUNT] DECREF on missing key: 234
   Failed to delete remote object: 12
   ❌ ERRORS PRESENT! Investigation required.

3. Last 5 errors:
   WARNING: [REFCOUNT] DECREF on missing key ('builtins.method', 10665440, 123474125771264)
   WARNING: Failed to delete remote object ('builtins.dict', 10733632, 139475418934272)
   ...

=== Check complete ===
```

**This is a problem!** You need to:
1. Save the full log for analysis
2. Check when the errors started
3. Correlate them with events (large requests, long-running work, etc.)

## What to Do When Errors Are Detected

### 1. Save the Logs

```bash
cp <stderr-log> ~/refcount_errors_$(date +%Y%m%d_%H%M%S).log
```

### 2. Analyze the Patterns

**Which objects are most common?**
```bash
grep "DECREF on missing key" ~/refcount_errors_*.log | \
  sed "s/.*('\([^']*\)'.*/\1/" | sort | uniq -c | sort -rn
```

**When do they occur?**
```bash
grep -E "REFCOUNT|Failed to delete" ~/refcount_errors_*.log | head -20
```

### 3. Correlate with Load

- Check the log file size: `ls -lh <stderr-log>`
- If the log is huge → many errors → high load
- If the log is small → rare errors → a specific bug

### 4. Reporting

Create an issue with the following information:
- Number of errors
- Object types (bound method, dict, list, etc.)
- Time period
- Conditions of occurrence (if known)
- Attach log fragments

## Disabling Monitoring (NOT RECOMMENDED)

If for some reason you need to disable error monitoring in stderr, you can redirect stderr to /dev/null when starting the process:

```bash
<start-command> 2>/dev/null
```

**BUT THIS IS A BAD IDEA!** You will lose important diagnostic information.

## Summary

**Now you will ALWAYS see refcount errors** in the host process's stderr log.

**Check after the next start of the process:**
```bash
# 1. Start/restart the process

# 2. Wait a little for initialization
sleep 2

# 3. Verify that monitoring is active
tail -20 <stderr-log> | grep "INFO:"

# Expected output:
# INFO: RPyC Connection conn1 initialized. Refcount error monitoring: ENABLED (errors always logged to stderr)
```

**If you see this message** - everything works! 🎉

**If there are no refcount errors** - excellent! The system works correctly! ✅

**If there are errors** - now you can see them and investigate! 🔍
