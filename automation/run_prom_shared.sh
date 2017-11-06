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
echo "export MONGO_IP=$MONGO_HOST" >> $USER_HOME/.bashrc

# edit luigi.cfg to contain the new job ID and file location
sed -i -e 's/<RUN_ID>/$RUN_ID/'\
       -e 's/<FILE_NAME>/$FILE_NAME/'\
       -e 's/<SFTP_HOST>/$SFTP_HOST/'\
       -e 's/<KEY_PAIR>/$KEY_PAIR/'\
       -e 's/<MONGO_HOST>/$MONGO_HOST/'\
       -e 's/<MYSQL_HOST>/$MYSQL_HOST/'\
       -e 's/<MYSQL_USER>/$MYSQL_USER/'\
       -e 's/<MYSQL_PASS>/$MYSQL_PASS/'\
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