# Filer Directory Creation Debugging

## Issue
Step 2.5 fails with "Failed to create filer directory" but provides no error details.

## Root Cause
The error output was being filtered by `grep` commands, hiding the actual failure reason.

## Fix Applied

### Enhanced Error Logging
Added comprehensive debugging to Step 2.5:

1. **SSH Connection Test**
   - Tests SSH connection before attempting mkdir
   - Runs simple `echo 'SSH_TEST_OK'` command
   - If fails, shows detailed connection error

2. **Detailed Error Capture**
   - Captures full stdout/stderr from mkdir command
   - Filters only noise (StrictHostKeyChecking warnings)
   - Shows actual error messages

3. **Directory Verification**
   - After successful creation, verifies directory exists
   - Runs `[ -d '$FILER_TARGET_PATH' ] && echo 'EXISTS'`
   - Warns if creation succeeded but verification fails

### Debug Output Now Shows

```bash
Testing SSH connection to filer...
✓ SSH connection to filer verified
Creating directory: /home/nutanix/data/Bugs/MA/NCM2_1/...
✓ Directory created successfully
✓ Directory verified on filer: /home/nutanix/data/Bugs/...
```

### If SSH Fails, Shows:
```
✗ Cannot connect to filer via SSH (exit: X)
Output: [full error message]
Check:
  1. Filer host is reachable: 10.46.1.165
  2. SSH is enabled on filer
  3. Password is correct for user: nutanix
```

### If mkdir Fails, Shows:
```
✗ Failed to create directory (exit code: X)
Command: mkdir -p '/path/to/dir'
Error: [filtered error]
Full output: [complete output including any hidden errors]
```

## Common Failure Scenarios

### 1. Wrong Password
**Symptom**: SSH test fails with "Permission denied"
**Fix**: Check FILER_PASSWORD in script or UI

### 2. Host Unreachable
**Symptom**: SSH test fails with "Connection refused" or "No route to host"
**Fix**: Verify FILER_HOST IP address and network connectivity

### 3. Permission Denied on mkdir
**Symptom**: SSH test passes, mkdir fails with "Permission denied"
**Fix**: Check if FILER_USER has write permissions to parent directory

### 4. Parent Directory Missing
**Symptom**: mkdir fails with "No such file or directory"
**Fix**: Ensure parent path exists: `/home/nutanix/data/Bugs/MA/NCM2_1/`

## Testing

Run a new log collection job. Step 2.5 will now show:
- Clear SSH connection status
- Exact error messages if anything fails
- Directory verification after creation

The enhanced logging will pinpoint the exact issue.
