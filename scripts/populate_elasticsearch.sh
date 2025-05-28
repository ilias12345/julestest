#!/bin/bash

# Elasticsearch connection details
ES_HOST="localhost:9200"
INDEX_NAME="isocial_deposits"

# Calculate timestamps
DATE_NOW_MS=$(($(date +%s%N)/1000000))
NINETY_DAYS_AGO_MS=$(($DATE_NOW_MS - 90 * 24 * 60 * 60 * 1000))
HUNDRED_DAYS_AGO_MS=$(($DATE_NOW_MS - 100 * 24 * 60 * 60 * 1000))
TEN_DAYS_AGO_MS=$(($DATE_NOW_MS - 10 * 24 * 60 * 60 * 1000))
FIVE_DAYS_AGO_MS=$(($DATE_NOW_MS - 5 * 24 * 60 * 60 * 1000))

echo "Current Epoch MS: $DATE_NOW_MS"
echo "90 Days Ago MS: $NINETY_DAYS_AGO_MS"
echo "100 Days Ago MS: $HUNDRED_DAYS_AGO_MS"
echo "10 Days Ago MS: $TEN_DAYS_AGO_MS"
echo "5 Days Ago MS: $FIVE_DAYS_AGO_MS"

# 1. Delete the index if it exists (for a clean state)
echo "Deleting index $INDEX_NAME if it exists..."
curl -X DELETE "${ES_HOST}/${INDEX_NAME}"
echo "" # Newline for better output

# 2. Create the index with mapping for ISocialDepositEpoch
echo "Creating index $INDEX_NAME with mapping for ISocialDepositEpoch..."
curl -X PUT "${ES_HOST}/${INDEX_NAME}" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "ISocialDepositId": { "type": "keyword" },
      "ISocialDepositMessageName": { "type": "text" },
      "ISocialDepositOwner": { "type": "keyword" },
      "ISocialDepositText": { "type": "text" },
      "ISocialDepositEpoch": { "type": "long" },
      "ISocialDepositMetadata": {
        "type": "object",
        "properties": {
          "source": { "type": "keyword" },
          "tags": { "type": "keyword" }
        }
      }
    }
  }
}
'
echo "" # Newline

# 3. Index sample documents
echo "Indexing sample documents..."

# Document 1 (older than 90 days)
curl -X POST "${ES_HOST}/${INDEX_NAME}/_doc/1" -H 'Content-Type: application/json' -d'
{
  "ISocialDepositId": "doc001",
  "ISocialDepositMessageName": "Old Message 1",
  "ISocialDepositOwner": "user_alpha",
  "ISocialDepositText": "This is an old deposit, more than 90 days.",
  "ISocialDepositEpoch": '$HUNDRED_DAYS_AGO_MS',
  "ISocialDepositMetadata": {
    "source": "system_A",
    "tags": ["archive", "historical"]
  }
}
'
echo "" # Newline

# Document 2 (older than 90 days)
curl -X POST "${ES_HOST}/${INDEX_NAME}/_doc/2" -H 'Content-Type: application/json' -d'
{
  "ISocialDepositId": "doc002",
  "ISocialDepositMessageName": "Old Message 2",
  "ISocialDepositOwner": "user_beta",
  "ISocialDepositText": "Another old deposit, exactly 90 days ago.",
  "ISocialDepositEpoch": '$NINETY_DAYS_AGO_MS',
  "ISocialDepositMetadata": {
    "source": "system_B",
    "tags": ["archive", "legacy"]
  }
}
'
echo "" # Newline

# Document 3 (more recent than 90 days)
curl -X POST "${ES_HOST}/${INDEX_NAME}/_doc/3" -H 'Content-Type: application/json' -d'
{
  "ISocialDepositId": "doc003",
  "ISocialDepositMessageName": "Recent Message 1",
  "ISocialDepositOwner": "user_gamma",
  "ISocialDepositText": "This is a recent deposit, 10 days ago.",
  "ISocialDepositEpoch": '$TEN_DAYS_AGO_MS',
  "ISocialDepositMetadata": {
    "source": "system_C",
    "tags": ["active", "current"]
  }
}
'
echo "" # Newline

# Document 4 (more recent than 90 days)
curl -X POST "${ES_HOST}/${INDEX_NAME}/_doc/4" -H 'Content-Type: application/json' -d'
{
  "ISocialDepositId": "doc004",
  "ISocialDepositMessageName": "Recent Message 2",
  "ISocialDepositOwner": "user_delta",
  "ISocialDepositText": "A very recent deposit, from 5 days ago.",
  "ISocialDepositEpoch": '$FIVE_DAYS_AGO_MS',
  "ISocialDepositMetadata": {
    "source": "system_A",
    "tags": ["active", "fresh"]
  }
}
'
echo "" # Newline

# Document 5 (current time)
curl -X POST "${ES_HOST}/${INDEX_NAME}/_doc/5" -H 'Content-Type: application/json' -d'
{
  "ISocialDepositId": "doc005",
  "ISocialDepositMessageName": "Current Time Message",
  "ISocialDepositOwner": "user_epsilon",
  "ISocialDepositText": "A deposit made right now.",
  "ISocialDepositEpoch": '$DATE_NOW_MS',
  "ISocialDepositMetadata": {
    "source": "system_B",
    "tags": ["live", "realtime"]
  }
}
'
echo "" # Newline

echo "Finished populating Elasticsearch index $INDEX_NAME."

# Make the script executable (can also be done from command line once)
# chmod +x scripts/populate_elasticsearch.sh
# The user will run this, or it can be run during a build step if needed.
# For the purpose of this task, the file is created. Making it executable is a runtime action.
