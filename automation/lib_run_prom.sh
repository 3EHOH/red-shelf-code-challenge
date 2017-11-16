#!/bin/sh


# verifies that a variable has been defined in aws_config.cfg
check_def() {
    if [[ ! -v $1 ]]; then
        echo "Required variable $1 is not defined, check your config file"
        exit -1
    fi
}



# launches a new EC2 instance
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
    
    # launch the instance
    local LAUNCH_COMMAND=$(cat <<LAUNCH_COMMAND_ARGS
aws ec2 run-instances \
    --image-id "$1" \
    --count 1 \
    --instance-type "$2" \
    --key-name "$3" \
    --security-group-ids $4 \
    --subnet-id "$5" \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=$6}]' \
    $USER_DATA \
> $LAUNCH_COMMAND_DIR/${6}__instance.launch
LAUNCH_COMMAND_ARGS
)
    echo "$LAUNCH_COMMAND"
    eval "$LAUNCH_COMMAND"
}




# if no command-line parameters are provided, print usage message - 
# requires SCRIPT_NAME and SCRIPT_DESC to be set in wrapper script
if [[ $CONFIG_FILE == "" || $RUN_ID == "" || $FILE_NAME == "" ]]; then
    cat <<EOF
Usage: $SCRIPT_NAME <aws_config_file> <custom_run_id> <sftp_file_name>

$SCRIPT_DESC

Arguments:
  <aws_config_file> is a file that contains AMI IDs, instance types, and security
    groups that will be used for launching servers. See example_aws_config.cfg
  <custom_run_id> is a user-defined identifier for this run. It will be used in the
    name of the EC2 instance, and for reporting. It is not used by the actual
    Prometheus code, only the automation script. It must only contain letters, numbers,
    and hyphens.
  <sftp_file_name> should be the name of the input .zip file on the SFTP server

e.g. './$SCRIPT_NAME aws_shared_config.cfg my-run-1234 50K_Test_2016_03_16.zip'
EOF
    exit
fi


# load the config file
source $CONFIG_FILE


# load CONFIG_FILE and check variables
check_def PAYFORMANCE_HOME
check_def KEY_PAIR
check_def SFTP_HOST
check_def DISTRIBUTED_MODE
check_def MYSQL_USER
check_def MYSQL_PASS
check_def ROOT_AMI_ID
check_def ROOT_INSTANCE_TYPE
check_def ROOT_SECURITY_GROUPS
check_def SUBNET_ID
check_def MYSQL_AMI_ID
check_def MONGO_AMI_ID
check_def MYSQL_INSTANCE_TYPE
check_def MONGO_INSTANCE_TYPE
check_def MYSQL_SECURITY_GROUPS
check_def MONGO_SECURITY_GROUPS



# seconds to sleep after starting a new server
SLEEP_SECONDS=300

# TO-DO - Check that RUN_ID only contains letters, numbers, and hyphens

FILE_DIR=`basename -s .zip $FILE_NAME`
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
DOWNLOAD_COMMAND="$SFTP_COMMAND ${SFTP_USER}@${SFTP_HOST}:$DOWNLOAD_SFTP_FILE_PATH $DOWNLOAD_FILE"

# local directory that contains output from AWS instance launching commands
LOG_DIR="/tmp/$RUN_ID"
LAUNCH_COMMAND_FILE="${RUN_ID}"
LAUNCH_COMMAND_DIR="$LOG_DIR/launch"

# check to see if this run is already in use
if [[ -d "$LOG_DIR" ]]; then
    echo "Run with ID '$RUN_ID' is already in use"
    exit
else
    mkdir "$LOG_DIR"
    mkdir "$LAUNCH_COMMAND_DIR"
fi

# now that everything is defined, set the database hosts
# this may point to localhost, hardcoded IPs, or newly-spawned EC2 instances
set_db_hosts

# this script will be run as root after the EC2 instance is launched
ROOT_LAUNCH_SCRIPT_FILE="$LAUNCH_COMMAND_DIR/${RUN_ID}__root.sh"
ROOT_INSTANCE_NAME="${RUN_ID}__root"
cat <<EOF > "$ROOT_LAUNCH_SCRIPT_FILE"
#!/bin/bash

$DOWNLOAD_COMMAND

# create output directory
sudo -u $EC2_USER mkdir $OUTPUT_DIR

unzip -d $DOWNLOAD_DIR $DOWNLOAD_FILE

echo "export HOSTNAME=$ROOT_INSTANCE_NAME" >> $USER_HOME/.bashrc
echo "export MONGO_HOST=$MONGO_HOST" >> $USER_HOME/.bashrc

# edit luigi.cfg to contain the new job ID and file location
sed -i -e 's/<RUN_ID>/$RUN_ID/'\
       -e 's/<FILE_NAME>/$FILE_NAME/'\
       -e 's|<FILE_DIR>|$FILE_DIR|'\
       -e 's/<SFTP_HOST>/$SFTP_HOST/'\
       -e 's/<KEY_PAIR>/$KEY_PAIR/'\
       -e 's|<PAYFORMANCE_HOME>|$PAYFORMANCE_HOME|'\
       -e 's/<DISTRIBUTED_MODE>/$DISTRIBUTED_MODE/'\
       -e 's/<MONGO_HOST>/$MONGO_HOST/'\
       -e 's/<MYSQL_HOST>/$MYSQL_HOST/'\
       -e 's/<MYSQL_USER>/$MYSQL_USER/'\
       -e 's/<MYSQL_PASS>/$MYSQL_PASS/'\
       -e 's/<NORM_CHUNK_SIZE>/$NORM_CHUNK_SIZE/'\
       -e 's/<NORM_STOP_AFTER>/$NORM_STOP_AFTER/'\
       -e 's/<NORM_INSTANCE_COUNT>/$NORM_INSTANCE_COUNT/'\
       -e 's/<NORM_PROCESSES_PER_INSTANCE>/$NORM_PROCESSES_PER_INSTANCE/'\
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

# launch the root instance
launch_instance \
    "$ROOT_AMI_ID" \
    "$ROOT_INSTANCE_TYPE" \
    "$KEY_PAIR" \
    "$ROOT_SECURITY_GROUPS" \
    "$SUBNET_ID" \
    "$ROOT_INSTANCE_NAME" \
    "${ROOT_LAUNCH_SCRIPT_FILE}"

