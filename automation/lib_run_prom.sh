#!/bin/sh


check_def() {
    if [ -z "$1" ]; then
        echo "Required variable $1 is not defined, check your config file"
        exit -1
    fi
}

check_root_defined_vars() {
    check_def PAYFORMANCE_HOME
    check_def KEY_PAIR
    check_def SFTP_HOST
    check_def MYSQL_HOST
    check_def MYSQL_USER
    check_def MYSQL_PASS
    check_def MONGO_HOST
    check_def ROOT_AMI_ID
    check_def ROOT_INSTANCE_TYPE
    check_def ROOT_SECURITY_GROUPS
    check_def SUBNET_ID
}

check_shared_defined_vars() {
    check_def MYSQL_AMI_ID
    check_def MONGO_AMI_ID
    check_def MYSQL_INSTANCE_TYPE
    check_def MONGO_INSTANCE_TYPE
    check_def MYSQL_SECURITY_GROUPS
    check_def MONGO_SECURITY_GROUPS
}


launch_instance() {
    # $1 AMI ID
    # $2 INSTANCE_TYPE
    # $3 KEY_NAME
    # $4 SECURITY_GROUPS
    # $5 SUBNET_ID
    # $6 NAME
    # (optional) $7 user-data file
    
    local USER_DATA=''
    if [ -n "$7" ]; then
        USER_DATA="--user-data file://$7"
    fi
    
    # launch the root instance
    local LAUNCH_COMMAND=$(cat <<LAUNCH_COMMAND_ARGS
aws ec2 run-instances \
    --image-id "$1" \
    --count 1 \
    --instance-type "$2" \
    --key-name "$3" \
    --security-group-ids $4 \
    --subnet-id "$5" \
    --no-associate-public-ip-address \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=$6}]' \
    $USER_DATA \
> $LAUNCH_COMMAND_DIR/${6}__instance.launch
LAUNCH_COMMAND_ARGS
)
    echo "$LAUNCH_COMMAND"
    eval "$LAUNCH_COMMAND"
}

# seconds to sleep after starting a new server
SLEEP_SECONDS=300

# TO-DO - Check that RUN_ID only contains letters, numbers, and hyphens

RUN_ID="$2"
FILE_NAME="$3"
EC2_USER="ec2-user"
USER_HOME="/home/$EC2_USER"
ECR_HOME="/ecrfiles"
SFTP_FILE="/$USER_HOME/input/$FILE_NAME"

# SFTP configuration
SFTP_KEYFILE=$USER_HOME/.ssh/$KEY_PAIR.pem
SFTP_USER="$EC2_USER"
LUIGI_DIR="$PAYFORMANCE_HOME/luigi"

# output file and startup script file locations on worker servers
OUTPUT_DIR="$USER_HOME/${RUN_ID}__output"
OUTPUT_FILE="$OUTPUT_DIR/$RUN_ID.log"

# base arguments for scp
# sudo is necessary because the script will run as root on startup
SFTP_COMMAND="sudo -u $EC2_USER scp -i $SFTP_KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# location and command for copying the input .zip file to the runner instance
DOWNLOAD_DIR="$ECR_HOME/input"
DOWNLOAD_FILE="$DOWNLOAD_DIR/$FILE_NAME"
DOWNLOAD_SFTP_FILE_PATH="$SFTP_FILE"
DOWNLOAD_COMMAND="$SFTP_COMMAND ${SFTP_USER}@${SFTP_SERVER}:$DOWNLOAD_SFTP_FILE_PATH $DOWNLOAD_FILE"

# local directory that contains output from AWS instance launching commands
LAUNCH_COMMAND_FILE="${RUN_ID}__launch-commands"
LAUNCH_COMMAND_DIR="/tmp/$LAUNCH_COMMAND_FILE"


init_launch_commands() {
    # create output directory
    if [ -d "$LAUNCH_COMMAND_DIR" ]; then
        echo "Run with ID '$LAUNCH_COMMAND_DIR' is already in use"
        exit -1
    fi
    mkdir $LAUNCH_COMMAND_DIR
}

upload_launch_commands() {
    # location and command for copying output files back to to the file server
    local UPLOAD_FILE="$USER_HOME/${RUN_ID}__output.zip"
    local UPLOAD_SFTP_FILE_PATH="${SFTP_FILE}__output.zip"
    local UPLOAD_COMMAND="$SFTP_COMMAND $UPLOAD_FILE ${SFTP_USER}@${SFTP_SERVER}:$UPLOAD_SFTP_FILE_PATH"
    
    local LAUNCH_UPLOAD_FILE=${LAUNCH_COMMAND_DIR}.zip
    zip -r $LAUNCH_UPLOAD_FILE $LAUNCH_COMMAND_DIR
    $SFTP_COMMAND $LAUNCH_UPLOAD_FILE ${SFTP_USER}@${SFTP_HOST}:${LAUNCH_COMMAND_FILE}.zip
}





