#!/bin/bash

# 1. Set command-line input parameters
CONFIG_FILE="$1"
RUN_ID="$2"
FILE_NAME="$3"

# 2. Descriptive data used in lib_run_prom.sh if no parameters are provided
SCRIPT_NAME="run_prom_standalone.sh"
SCRIPT_DESC="Runs Prometheus with all Norman, Connie, and DB processes on one server"

# 3. callback function that will be used to determine database host addresses
set_db_hosts() {
    MYSQL_HOST=localhost
    MONGO_HOST=localhost
}

# 4. if true, runs normans/connies on separate EC2 instances
DISTRIBUTED_MODE=false

# 5. Run the launcher
source lib_run_prom.sh

