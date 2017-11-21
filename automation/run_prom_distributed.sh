#!/bin/bash

# 1. Set command-line input parameters
CONFIG_FILE="$1"
RUN_ID="$2"
FILE_NAME="$3"

# 2. Descriptive data used in lib_run_prom.sh if no parameters are provided
SCRIPT_NAME="run_prom_distributed.sh"
SCRIPT_DESC="Runs Prometheus with separate databases and all Norman and Connie processes on separate EC2 instances"

# 3. callback function that will be used to determine database host addresses
set_db_hosts() {
    # launch the MySQL instance
    MYSQL_INSTANCE_NAME="${RUN_ID}__mysql"
    launch_instance \
        "$MYSQL_AMI_ID" \
        "$MYSQL_INSTANCE_TYPE" \
        "$KEY_PAIR" \
        "$MYSQL_SECURITY_GROUPS" \
        "$SUBNET_ID" \
        "$MYSQL_INSTANCE_NAME"

    # launch the MySQL instance
    MONGO_INSTANCE_NAME="${RUN_ID}__mongo"
    launch_instance \
        "$MONGO_AMI_ID" \
        "$MONGO_INSTANCE_TYPE" \
        "$KEY_PAIR" \
        "$MONGO_SECURITY_GROUPS" \
        "$SUBNET_ID" \
        "$MONGO_INSTANCE_NAME"
    # sleep while we wait for database servers to reach running state
    sleep $SLEEP_SECONDS

    # capture the server IP addresses so we can rewrite database.properties
    MYSQL_HOST=$(python find_server_ip.py $MYSQL_INSTANCE_NAME)
    MONGO_HOST=$(python find_server_ip.py $MONGO_INSTANCE_NAME)

    echo "MySQL: $MYSQL_HOST"
    echo "Mongo: $MONGO_HOST"
}

# 4. if true, runs normans/connies on separate EC2 instances
DISTRIBUTED_MODE=true

# 5. Run the launcher
source lib_run_prom.sh