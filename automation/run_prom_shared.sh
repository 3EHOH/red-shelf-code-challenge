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

# load library
source lib_run_prom.sh

check_root_defined_vars
check_shared_defined_vars

init_launch_commands

# launch the MySQL instance
MYSQL_INSTANCE_NAME="${RUN_ID}__mysql"
MYSQL_LAUNCH_COMMAND=$(cat <<MYSQL_LAUNCH_COMMAND
aws ec2 run-instances \
    --image-id $MYSQL_AMI_ID \
    --count 1 \
    --instance-type "$MYSQL_INSTANCE_TYPE" \
    --key-name "$KEY_PAIR" \
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

#capture the mysql ip address so we can rewrite database.properties with its value
MYSQL_IP=$(python find_server_ip.py $MYSQL_INSTANCE_NAME)


# launch the mongo instance
MONGO_INSTANCE_NAME="${RUN_ID}__mongo"
MONGO_LAUNCH_COMMAND=$(cat <<MONGO_LAUNCH_COMMAND
aws ec2 run-instances \
    --image-id $MONGO_AMI_ID \
    --count 1 \
    --instance-type "$MONGO_INSTANCE_TYPE" \
    --key-name "$KEY_PAIR" \
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
       -e 's/<SFTP_HOST>/$SFTP_HOST/'\
       -e 's/<KEY_PAIR>/$KEY_PAIR/'\
       -e 's/<MONGO_HOST>/$MONGO_IP/'\
       -e 's/<MYSQL_HOST>/$MYSQL_IP/'\
       -e 's/<MYSQL_USER>/$MYSQL_USER/'\
       -e 's/<MYSQL_PASS>/$MYSQL_PASS/'\
    $LUIGI_DIR/luigi.cfg

# edit database.properties
sed -i -e 's/<MONGO_HOST>/$MONGO_IP/'\
       -e 's/<MYSQL_HOST>/$MYSQL_IP/'\
       -e 's/<MYSQL_USER>/$MYSQL_USER/'\
       -e 's/<MYSQL_PASS>/$MYSQL_PASS/'\
    $LUIGI_DIR/database.properties
cp $LUIGI_DIR/database.properties $ECR_HOME/scripts/database.properties

sudo -u $EC2_USER $LUIGI_DIR/doit.sh > $OUTPUT_DIR/${RUN_ID}__luigi.log 2>&1

EOF


launch_instance \
    $ROOT_AMI_ID \
    "$ROOT_INSTANCE_TYPE" \
    "$KEY_PAIR" \
    "$ROOT_SECURITY_GROUPS" \
    "$SUBNET_ID" \
    "$ROOT_INSTANCE_NAME" \
    "${ROOT_LAUNCH_SCRIPT_FILE}"

upload_launch_commands


