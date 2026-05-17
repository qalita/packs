#!/bin/bash

# Function to scan and list directories inside the 'data/' directory for TEST_DATASETS
generate_test_datasets() {
    local data_dir="$1"
    TEST_DATASETS=()  # Initialize an empty array
    for dir in "${data_dir}"/*; do
        if [ -d "$dir" ]; then
            dir_name=$(basename "$dir")
            TEST_DATASETS+=("$dir_name")
        fi
    done
}

current_datetime=$(date +"%Y%m%d")

process_data_pack() {
    local dataset="$1"
    local pack="$2"
    # Correctly determine dataset directory
    local dataset_dir="${DATA_DIR}/${dataset}"
    mkdir -p "${dataset_dir}/output/${current_datetime}"
    log_file="${dataset_dir}/output/${current_datetime}/${dataset}_${pack}.log"
    # Redirect the outputs of this iteration to the log file
    {
        echo "Processing Dataset: ${dataset} with Test Pack: ${pack}"

        # 1. Copy source_conf.json from the dataset directory into the current test pack directory
        cp "${DATA_DIR}/${dataset}/source_conf.json" "${ROOT_DIR}/${pack}"

        # 2. Change to the test pack directory
        cd "${ROOT_DIR}/${pack}" || return

        # 3. Execute the run.sh script
        if [ -x "./run.sh" ]; then
            ./run.sh
        else
            echo "run.sh is not executable or not found in $pack. Skipping."
            return
        fi

        # 4. Once processing is done, copy the result files back into the dataset directory
        cp metrics.json "${DATA_DIR}/${dataset}/output/${current_datetime}/${pack}_metrics.json"
        cp recommendations.json "${DATA_DIR}/${dataset}/output/${current_datetime}/${pack}_recommendations.json"
        cp schemas.json "${DATA_DIR}/${dataset}/output/${current_datetime}/${pack}_schemas.json"
        cp *.html "${DATA_DIR}/${dataset}/output/${current_datetime}/"
        cp *.xlsx "${DATA_DIR}/${dataset}/output/${current_datetime}/"
        cp *.csv "${DATA_DIR}/${dataset}/output/${current_datetime}/"


        # 5. Print a completion message
        echo "Completed: ${dataset} with ${pack}"

        # 6. Cleanup
        # Remove all .json files except pack_conf.json
        find "${ROOT_DIR}/${pack}/" -type f -name "*.json" ! -name "pack_conf.json" -exec rm {} +

        # Remove all .html files
        find "${ROOT_DIR}/${pack}/" -type f -name "*.html" -exec rm {} +

        # Print a message shortly after cleanup process. This is optional.
        echo "Cleanup done for $pack."
    } &> "${log_file}"

    # Return to the root directory for the next iteration
    cd "${ROOT_DIR}"
}

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <pack> [dataset]"
    exit 1
fi

PACK=$1
ROOT_DIR=$(pwd)
DATA_DIR="${ROOT_DIR}/data"

# Check if the second argument is provided (non-empty)
if [ -n "$2" ]; then
    DATASETS=("$2")
else
    generate_test_datasets "${DATA_DIR}"
    DATASETS=("${TEST_DATASETS[@]}")
fi

for dataset in "${DATASETS[@]}"; do
    process_data_pack "${dataset}" "${PACK}"
done

echo "All Tests Completed for ${PACK} pack."
