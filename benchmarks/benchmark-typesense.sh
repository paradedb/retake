#!/bin/bash

# Exit on subcommand errors
set -Eeuo pipefail

# Ensure the "out" directory exists
mkdir -p out

# shellcheck disable=SC1091
source "helpers/get_data.sh"

PORT=8108
TS_VERSION=0.25.1
WIKI_ARTICLES_FILE=wiki-articles.json
TYPESENSE_API_KEY=xyz
TYPESENSE_DATA=$(pwd)/typesense-data
OUTPUT_CSV=out/benchmark_typesense.csv

# Cleanup function to stop and remove the Docker container
cleanup() {
  echo ""
  echo "Cleaning up benchmark environment..."
  if docker ps -q --filter "name=typesense" | grep -q .; then
    docker kill typesense
  fi
  docker rm typesense
  rm -rf "$TYPESENSE_DATA"
  echo "Done!"
}

# Register the cleanup function to run when the script exits
trap cleanup EXIT

echo ""
echo "*******************************************************"
echo "* Benchmarking Typesense version: $TS_VERSION"
echo "*******************************************************"
echo ""

# Download and run docker container for Typesense
echo "Creating Typesense $TS_VERSION node..."
docker run \
  -d \
  --name typesense \
  -p $PORT:8108 \
  -v"$TYPESENSE_DATA:/data" "typesense/typesense:$TS_VERSION" \
  --data-dir /data \
  --api-key=$TYPESENSE_API_KEY \
  --enable-cors

# Wait for Docker container to spin up
echo ""
echo "Waiting for server to spin up..."
sleep 30
echo "Done!"

# Retrieve the benchmarking dataset
echo ""
echo "Retrieving dataset..."
download_data
echo "Done!"

# Output file for recording times
echo "Table Size,Index Time,Search Time" > $OUTPUT_CSV

# Table sizes to be processed (in number of rows). The maximum is 5M rows with the Wikipedia dataset
TABLE_SIZES=(10000 50000 100000 200000 300000 400000 500000 600000 700000 800000 900000 1000000 2000000 3000000 4000000 5000000)

for SIZE in "${TABLE_SIZES[@]}"; do
  echo ""
  echo "Running benchmarking suite on index with $SIZE documents..."

  # Create Typesense collection (only if it doesn't exist)
  COLLECTION_EXISTS=$(python3 helpers/typesense_requests.py collection_exists)
  if [ "$COLLECTION_EXISTS" -eq "0" ]; then
    echo "-- Creating Typesense collection..."
    python3 helpers/typesense_requests.py create_collection
  fi

  # Prepare data to be indexed by Typesense
  echo "-- Preparing data to be consumed by Typesense..."
  data_filename="${SIZE}_ts.json"
  head -n "$SIZE" "$WIKI_ARTICLES_FILE" > "$data_filename"

  # Time indexing
  echo "-- Loading data of size $SIZE into wikipedia_articles index..."
  echo "-- Timing indexing..."
  start_time=$( (time python3 helpers/typesense_requests.py bulk_import "$data_filename" > /dev/null) 2>&1 )
  index_time=$(echo "$start_time" | grep real | awk '{ split($2, array, "m|s"); print array[1]*60000 + array[2]*1000 }')

  # Time search
  echo "-- Timing search..."
  start_time=$( (time python3 helpers/typesense_requests.py search > /dev/null) 2>&1 )
  search_time=$(echo "$start_time" | grep real | awk '{ split($2, array, "m|s"); print array[1]*60000 + array[2]*1000 }')

  # Confirm document count
  doc_count=$(python3 helpers/typesense_requests.py num_documents)
  echo "-- Number of documents in wikipedia_articles index for size $SIZE: $doc_count"

  # Record times to CSV
  echo "$SIZE,$index_time,$search_time" >> $OUTPUT_CSV

  # Cleanup: delete the temporary data file
  echo "-- Cleaning up..."
  rm "$data_filename"
  echo ""
  echo "Done!"
done
