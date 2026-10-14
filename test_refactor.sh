#!/bin/bash
# Test script to validate ipfix_live.py refactoring

set -e

echo "=== IPFIX Live Keep-Alive Refactoring Tests ==="
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Test 1: Syntax Validation${NC}"
python3 -m py_compile ipfix_live.py
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Python syntax is valid${NC}"
else
    echo "✗ Syntax error found"
    exit 1
fi
echo ""

echo -e "${BLUE}Test 2: Import Validation${NC}"
python3 -c "import ipfix_live; print('✓ Module imports successfully')"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Module can be imported${NC}"
else
    echo "✗ Import failed"
    exit 1
fi
echo ""

echo -e "${BLUE}Test 3: Help Text (ensures argparse is intact)${NC}"
python3 ipfix_live.py --help > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Help text displays correctly${NC}"
else
    echo "✗ Help text failed"
    exit 1
fi
echo ""

echo -e "${BLUE}Test 4: Check --keep-alive flag exists${NC}"
python3 ipfix_live.py --help | grep -q "keep-alive"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ --keep-alive flag is present${NC}"
else
    echo "✗ --keep-alive flag missing"
    exit 1
fi
echo ""

echo -e "${BLUE}Test 5: Check --keepalive-interval flag exists${NC}"
python3 ipfix_live.py --help | grep -q "keepalive-interval"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ --keepalive-interval flag is present${NC}"
else
    echo "✗ --keepalive-interval flag missing"
    exit 1
fi
echo ""

echo -e "${BLUE}Test 6: Check --duration-limit flag exists${NC}"
python3 ipfix_live.py --help | grep -q "duration-limit"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ --duration-limit flag is present${NC}"
else
    echo "✗ --duration-limit flag missing"
    exit 1
fi
echo ""

echo -e "${BLUE}Test 7: Verify key functions exist${NC}"
python3 -c "
import ipfix_live
import inspect

# Check for critical functions
required_functions = ['flow_once', 'build_waves', 'log_line', 'main']
for func_name in required_functions:
    if not hasattr(ipfix_live, func_name):
        print(f'✗ Missing function: {func_name}')
        exit(1)
    
print('✓ All key functions present')
"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ All critical functions exist${NC}"
else
    echo "✗ Missing functions"
    exit 1
fi
echo ""

echo -e "${BLUE}Test 8: Check _TEARDOWN_REQUESTED event exists${NC}"
python3 -c "
import ipfix_live
import threading

if not hasattr(ipfix_live, '_TEARDOWN_REQUESTED'):
    print('✗ _TEARDOWN_REQUESTED not found')
    exit(1)

if not isinstance(ipfix_live._TEARDOWN_REQUESTED, threading.Event):
    print('✗ _TEARDOWN_REQUESTED is not a threading.Event')
    exit(1)

print('✓ _TEARDOWN_REQUESTED event exists and is correct type')
"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Threading event properly defined${NC}"
else
    echo "✗ Threading event issue"
    exit 1
fi
echo ""

echo -e "${BLUE}Test 9: Validate parallel mode options${NC}"
python3 ipfix_live.py --help | grep -q "parallel-mode"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Parallel mode options intact${NC}"
else
    echo "✗ Parallel mode missing"
    exit 1
fi
echo ""

echo -e "${BLUE}Test 10: Check code structure (no syntax/indent errors)${NC}"
python3 -m py_compile ipfix_live.py && python3 -c "
import ast
with open('ipfix_live.py', 'r') as f:
    code = f.read()
    try:
        ast.parse(code)
        print('✓ Code structure is valid')
    except SyntaxError as e:
        print(f'✗ Syntax error: {e}')
        exit(1)
"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Code structure validated${NC}"
else
    echo "✗ Code structure issue"
    exit 1
fi
echo ""

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}All validation tests passed!${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""
echo "Summary:"
echo "  ✓ Python syntax valid"
echo "  ✓ Module imports correctly"
echo "  ✓ All CLI flags present"
echo "  ✓ All functions exist"
echo "  ✓ Threading components intact"
echo "  ✓ Code structure validated"
echo ""
echo "Next steps:"
echo "  1. Test one-shot mode (no --keep-alive)"
echo "  2. Test keep-alive mode (with --keep-alive)"
echo "  3. Test graceful shutdown (kill -SIGUSR1)"
echo ""
echo "See KEEPALIVE_REFACTOR.md for detailed testing instructions."
