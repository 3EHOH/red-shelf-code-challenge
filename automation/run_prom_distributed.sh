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


echo "export HOSTNAME=$ROOT_INSTANCE_NAME" >> $USER_HOME/.bashrc
echo "export MONGO_HOST=$MONGO_HOST" >> $USER_HOME/.bashrc
echo "export MYSQL_HOST=$MYSQL_HOST" >> $USER_HOME/.bashrc
echo "export KEY_PAIR=$KEY_PAIR" >> $USER_HOME/.bashrc
echo "export NORMAN_AMI_ID=$NORMAN_AMI_ID" >> $USER_HOME/.bashrc
echo "export NORMAN_INSTANCE_TYPE=$NORMAN_INSTANCE_TYPE" >> $USER_HOME/.bashrc
echo "export CONNIE_AMI_ID=$CONNIE_AMI_ID" >> $USER_HOME/.bashrc
echo "export CONNIE_INSTANCE_TYPE=$CONNIE_INSTANCE_TYPE" >> $USER_HOME/.bashrc
echo "export ROOT_SECURITY_GROUPS='${ROOT_SECURITY_GROUPS[*]}'" >> $USER_HOME/.bashrc
echo "export MYSQL_SECURITY_GROUPS='${MYSQL_SECURITY_GROUPS[*]}'" >> $USER_HOME/.bashrc
echo "export MONGO_SECURITY_GROUPS='${MONGO_SECURITY_GROUPS[*]}'" >> $USER_HOME/.bashrc
echo "export LUIGI_DIR=$LUIGI_DIR" >> $USER_HOME/.bashrc

# edit luigi.cfg to contain the new job ID and file location
sed -i -e 's/<RUN_ID>/$RUN_ID/'\
       -e 's/<FILE_NAME>/$FILE_NAME/'\
       -e 's/<FILE_DIR>/$FILE_DIR/'\
       -e 's/<SFTP_HOST>/$SFTP_HOST/'\
       -e 's/<KEY_PAIR>/$KEY_PAIR/'\
       -e 's/<MONGO_HOST>/$MONGO_HOST/'\
       -e 's/<MYSQL_HOST>/$MYSQL_HOST/'\
       -e 's/<MYSQL_USER>/$MYSQL_USER/'\
       -e 's/<MYSQL_PASS>/$MYSQL_PASS/'\
       -e 's/<NORM_CHUNK_SIZE>/$NORM_CHUNK_SIZE/'\
       -e 's/<NORM_STOP_AFTER>/$NORM_STOP_AFTER/'\
       -e 's/<NORM_INSTANCE_COUNT>/$NORM_INSTANCE_COUNT/'\
       -e 's/<NORM_PROCESSES_PER_INSTANCE>/$NORM_PROCESSES_PER_INSTANCE/'\
       -e 's/<CONNIE_CHUNK_SIZE>/$CONNIE_CHUNK_SIZE/'\
       -e 's/<CONNIE_STOP_AFTER>/$CONNIE_STOP_AFTER/'\
       -e 's/<CONNIE_INSTANCE_COUNT>/$CONNIE_INSTANCE_COUNT/'\
       -e 's/<CONNIE_PROCESSES_PER_INSTANCE>/$CONNIE_PROCESSES_PER_INSTANCE/'\
    $LUIGI_DIR/luigi.cfg

# edit database.properties
sed -i -e 's/<MONGO_HOST>/$MONGO_HOST/'\
       -e 's/<MYSQL_HOST>/$MYSQL_HOST/'\
       -e 's/<MYSQL_USER>/$MYSQL_USER/'\
       -e 's/<MYSQL_PASS>/$MYSQL_PASS/'\
    $LUIGI_DIR/database.properties
cp $LUIGI_DIR/database.properties $ECR_HOME/scripts/database.properties

sudo -u $EC2_USER $LUIGI_DIR/doit.sh > $OUTPUT_DIR/${RUN_ID}__luigi.log 2>&1

EOF


launch_instance \
    "$ROOT_AMI_ID" \
    "$ROOT_INSTANCE_TYPE" \
    "$KEY_PAIR" \
    "$ROOT_SECURITY_GROUPS" \
    "$SUBNET_ID" \
    "$ROOT_INSTANCE_NAME" \
    "${ROOT_LAUNCH_SCRIPT_FILE}"

upload_launch_commands