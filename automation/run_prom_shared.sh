#!/bin/bash

# first: set up a user and group (go to IAM in the AWS console) http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
# The group should have full admin privileges

# instructions on running a script on startup: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/user-data.html

# check for arguments
if [ $# -lt 3 ]; then
    cat <<EOF
Usage: run_prom_shared.sh <aws_config_file> <custom_run_id> <sftp_file_name>

Runs Prometheus with MongoDB and MySQL on dedicated servers.

Arguments:
  <aws_config_file> is a file that contains AMI IDs, instance types, and security
    groups that will be used for launching servers. See example_aws_shared_config.cfg
  <custom_run_id> is a user-defined identifier for this run. It will be used in the
    name of the EC2 instance, and for reporting. It is not used by the actual
    Prometheus code, only the automation script. It must only contain letters, numbers,
    and hyphens.
  <sftp_file_name> should be the name of the input .zip file on the SFTP server

e.g. './run_prom_shared.sh aws_shared_config.cfg my-run-1234 50K_Test_2016_03_16.zip'
EOF
    exit
fi


# load AWS-specific configuration
# this is not safe, but will suffice for the moment
source $1

# TO-DO: check that AWS variables are defined


##########################################
#
# VARIABLE DEFINITIONS
#
# It should not be necessary to edit these variables
#
##########################################

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
SFTP_KEYFILE=$USER_HOME/.ssh/$KEY_NAME.pem
SFTP_USER="$EC2_USER"

LUIGI_DIR="$USER_HOME/payformance/luigi"

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

# location and command for copying output files back to to the file server
UPLOAD_FILE="$USER_HOME/${RUN_ID}__output.zip"
UPLOAD_SFTP_FILE_PATH="${SFTP_FILE}__output.zip"
UPLOAD_COMMAND="$SFTP_COMMAND $UPLOAD_FILE ${SFTP_USER}@${SFTP_SERVER}:$UPLOAD_SFTP_FILE_PATH"

# local directory that contains output from AWS instance launching commands
LAUNCH_COMMAND_FILE="$RUN_ID-launch-commands"
LAUNCH_COMMAND_DIR="/tmp/$LAUNCH_COMMAND_FILE"

# create output directory
mkdir $LAUNCH_COMMAND_DIR

# launch the MySQL instance
MYSQL_INSTANCE_NAME="${RUN_ID}__mysql"
MYSQL_LAUNCH_COMMAND=$(cat <<MYSQL_LAUNCH_COMMAND
aws ec2 run-instances \
    --image-id $MYSQL_AMI_ID \
    --count 1 \
    --instance-type "$MYSQL_INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids $MYSQL_SECURITY_GROUPS \
    --subnet-id "$SUBNET_ID" \
    --no-associate-public-ip-address \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=$MYSQL_INSTANCE_NAME}]' \
> $LAUNCH_COMMAND_DIR/mysql_instance.launch
MYSQL_LAUNCH_COMMAND
)

echo "$MYSQL_LAUNCH_COMMAND"

eval "$MYSQL_LAUNCH_COMMAND"

#sleep while we wait for mysql to reach running state
sleep $SLEEP_SECONDS

#capture the mongo ip address so we can rewrite database.properties with its value
MYSQL_IP=$(python find_server_ip.py $MYSQL_INSTANCE_NAME)


# launch the mongo instance
MONGO_INSTANCE_NAME="${RUN_ID}__mongo"
MONGO_LAUNCH_COMMAND=$(cat <<MONGO_LAUNCH_COMMAND
aws ec2 run-instances \
    --image-id $MONGO_AMI_ID \
    --count 1 \
    --instance-type "$MONGO_INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids $MONGO_SECURITY_GROUPS \
    --subnet-id "$SUBNET_ID" \
    --no-associate-public-ip-address \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=$MONGO_INSTANCE_NAME}]' \
> $LAUNCH_COMMAND_DIR/mongo_instance.launch
MONGO_LAUNCH_COMMAND
)

echo "$MONGO_LAUNCH_COMMAND"

eval "$MONGO_LAUNCH_COMMAND"

#sleep while we wait for mongo to reach running state
sleep $SLEEP_SECONDS

#capture the mongo ip address so we can rewrite database.properties with its value
MONGO_IP=$(python find_server_ip.py $MONGO_INSTANCE_NAME)

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
echo "export MONGO_IP=$MONGO_IP" >> $USER_HOME/.bashrc

# edit luigi.cfg to contain the new job ID and file location
sed -i -e 's/<RUN_ID>/$RUN_ID/'\
       -e 's/<FILE_NAME>/$FILE_NAME/'\
       -e 's/<SFTP_SERVER>/$SFTP_SERVER/'\
       -e 's/<MYSQL_SERVER>/$MYSQL_IP/'\
       -e 's/<KEY_NAME>/$KEY_NAME/'\
       -e 's/prd_host=.*/prd_host=$MYSQL_IP/'\
       -e 's/template_host=.*/template_host=$MYSQL_IP/'\
       -e 's/epb_host=.*/epb_host=$MYSQL_IP/'\
    $LUIGI_DIR/luigi.cfg

# edit database.properties to contain mysql ip
sed -i -e 's/md1.host=.*/md1.host=$MONGO_IP/'\
       -e 's/prd.host=.*/prd.host=$MYSQL_IP/'\
       -e 's/ecr.host=.*/ecr.host=$MYSQL_IP/'\
       -e 's/template.host=.*/template.host=$MYSQL_IP/'\
    $LUIGI_DIR/database.properties
cp $LUIGI_DIR/database.properties $ECR_HOME/scripts/database.properties


# set logging level
echo -e "\n\n[core]\nlog_level=INFO\n" >> $LUIGI_DIR/luigi.cfg

# ensure that local database servers are not running
service mysqld stop
service mongod stop

# run the luigi workflow - filter out DEBUG (logging levels aren't respected for stdout somehow)
sudo -u $EC2_USER $LUIGI_DIR/doit.sh | grep -v DEBUG > $LUIGI_DIR/logs/${RUN_ID}__luigi.log 2>&1

EOF



# launch the root instance
ROOT_LAUNCH_COMMAND=$(cat <<ROOT_LAUNCH_COMMAND
aws ec2 run-instances \
    --image-id $ROOT_AMI_ID \
    --count 1 \
    --instance-type "$ROOT_INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids $ROOT_SECURITY_GROUPS \
    --subnet-id "$SUBNET_ID" \
    --user-data "file://$ROOT_LAUNCH_SCRIPT_FILE" \
    --no-associate-public-ip-address \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=$ROOT_INSTANCE_NAME}]' \
> $LAUNCH_COMMAND_DIR/root_instance.launch
ROOT_LAUNCH_COMMAND
)

echo "$ROOT_LAUNCH_COMMAND"

eval "$ROOT_LAUNCH_COMMAND"


# copy launch information to the SFTP server
LAUNCH_UPLOAD_FILE=${LAUNCH_COMMAND_DIR}.zip
zip -r $LAUNCH_UPLOAD_FILE $LAUNCH_COMMAND_DIR
$SFTP_COMMAND $LAUNCH_UPLOAD_FILE ${SFTP_USER}@${SFTP_SERVER}:${SFTP_FILE}-${LAUNCH_COMMAND_FILE}.zip
#rm -rf $LAUNCH_COMMAND_DIR $LAUNCH_UPLOAD_FILE


