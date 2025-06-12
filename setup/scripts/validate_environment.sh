#!/bin/bash

echo "🔍 Validating environment configuration..."

# Source environment if file exists
if [ -f "setup/environment.env" ]; then
    source setup/environment.env
else
    echo "⚠️  Environment file not found at setup/environment.env"
    echo "💡 Copy setup/templates/environment.template to setup/environment.env"
    exit 1
fi

# Check required variables
check_var() {
    local var_name=$1
    local var_value="${!var_name}"
    
    if [ -z "$var_value" ]; then
        echo "❌ Missing: $var_name"
        return 1
    else
        echo "✅ Found: $var_name"
        return 0
    fi
}

ERRORS=0

# Critical variables
check_var "PROJECT_ID" || ((ERRORS++))
check_var "COMPOSER_ENV_NAME" || ((ERRORS++))
check_var "BQ_STAGING_DATASET" || ((ERRORS++))
check_var "GCS_BUCKET_NAME" || ((ERRORS++))
check_var "API_BASE_URL" || ((ERRORS++))

echo ""
echo "🧪 Testing service connectivity..."

# Test BigQuery
bq ls --project_id=$PROJECT_ID > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ BigQuery: Accessible"
else
    echo "❌ BigQuery: Connection failed"
    ((ERRORS++))
fi

# Test GCS
gsutil ls gs://$GCS_BUCKET_NAME/ > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ GCS: Accessible"
else
    echo "❌ GCS: Connection failed"
    ((ERRORS++))
fi

# Test API
curl -s --max-time 10 "$API_BASE_URL$API_PRODUCTS_ENDPOINT?limit=1" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ API: Responding"
else
    echo "❌ API: Connection failed"
    ((ERRORS++))
fi

echo ""
if [ $ERRORS -eq 0 ]; then
    echo "🎉 Environment validation passed!"
    exit 0
else
    echo "⚠️  Found $ERRORS validation errors"
    exit 1
fi