#!/bin/bash
# Test script for the AeroSense REST API
# Run this after starting the API: python src/api/app.py
# Requires curl and python3 with json.tool

BASE_URL="http://localhost:5000"
PASS=0
FAIL=0

green() { echo -e "\033[32m$1\033[0m"; }
red() { echo -e "\033[31m$1\033[0m"; }

test_endpoint() {
    local description="$1"
    local method="$2"
    local url="$3"
    local data="$4"
    local expected_code="$5"

    if [ -n "$data" ]; then
        response=$(curl -s -w "\n%{http_code}" -X "$method" "$url" -H "Content-Type: application/json" -d "$data")
    else
        response=$(curl -s -w "\n%{http_code}" -X "$method" "$url")
    fi

    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | sed '$d')

    if [ "$http_code" = "$expected_code" ]; then
        green "PASS: $description (HTTP $http_code)"
        PASS=$((PASS + 1))
    else
        red "FAIL: $description (expected $expected_code, got $http_code)"
        FAIL=$((FAIL + 1))
    fi
    echo "$body" | python3 -m json.tool 2>/dev/null || echo "$body"
    echo
}

echo "============================================"
echo "  AeroSense API Test Suite"
echo "============================================"
echo

# Test 1: Health check
test_endpoint "Health check" "GET" "$BASE_URL/api/v1/health" "" "200"

# Test 2: List sensors
test_endpoint "List sensors" "GET" "$BASE_URL/api/v1/sensors" "" "200"

# Test 3: Get latest temperature reading (may 404 if no data)
test_endpoint "Latest temperature reading" "GET" "$BASE_URL/api/v1/sensors/temperature/latest" "" "200"

# Test 4: Get latest humidity reading
test_endpoint "Latest humidity reading" "GET" "$BASE_URL/api/v1/sensors/humidity/latest" "" "200"

# Test 5: Get latest pressure reading
test_endpoint "Latest pressure reading" "GET" "$BASE_URL/api/v1/sensors/pressure/latest" "" "200"

# Test 6: Get stats for temperature (7 days)
test_endpoint "Stats for temperature (7 days)" "GET" "$BASE_URL/api/v1/sensors/temperature/stats?days=7" "" "200"

# Test 7: Get stats with invalid days parameter
test_endpoint "Stats invalid days (0)" "GET" "$BASE_URL/api/v1/sensors/temperature/stats?days=0" "" "400"

# Test 8: Get stats with days > 90
test_endpoint "Stats days > 90" "GET" "$BASE_URL/api/v1/sensors/temperature/stats?days=100" "" "400"

# Test 9: Get stats for unknown sensor
test_endpoint "Stats unknown sensor" "GET" "$BASE_URL/api/v1/sensors/light/latest" "" "404"

# Test 10: List anomalies (no filter)
test_endpoint "List anomalies (no filter)" "GET" "$BASE_URL/api/v1/anomalies" "" "200"

# Test 11: List anomalies filtered by sensor
test_endpoint "List anomalies (temperature)" "GET" "$BASE_URL/api/v1/anomalies?sensor=temperature&limit=5" "" "200"

# Test 12: List anomalies with invalid sensor
test_endpoint "List anomalies invalid sensor" "GET" "$BASE_URL/api/v1/anomalies?sensor=invalid" "" "400"

# Test 13: Post a valid reading
test_endpoint "Post valid reading" "POST" "$BASE_URL/api/v1/readings" \
    '{"sensor": "temperature", "value": 23.5, "source": "test-script"}' "201"

# Test 14: Post reading with missing sensor
test_endpoint "Post missing sensor" "POST" "$BASE_URL/api/v1/readings" \
    '{"value": 23.5}' "400"

# Test 15: Post reading with invalid sensor type
test_endpoint "Post invalid sensor type" "POST" "$BASE_URL/api/v1/readings" \
    '{"sensor": "light", "value": 100}' "422"

# Test 16: Post reading with non-numeric value
test_endpoint "Post non-numeric value" "POST" "$BASE_URL/api/v1/readings" \
    '{"sensor": "temperature", "value": "hot"}' "422"

# Test 17: 404 handler
test_endpoint "404 handler" "GET" "$BASE_URL/api/v1/nonexistent" "" "404"

# Test 18: 405 handler
test_endpoint "405 handler" "POST" "$BASE_URL/api/v1/health" "" "405"

echo "============================================"
echo "  Results: $PASS passed, $FAIL failed"
echo "============================================"
