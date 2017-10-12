#!/bin/bash

# first: set up a user and group (go to IAM in the AWS console) http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
# The group should have full admin privileges

# instructions on running a script on startup: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/user-data.html

# check for arguments
if [ $# -lt 3 ]; then
    echo "Usage: run_prom.sh <aws_config_file> <custom_job_id> <sftp_file_name>"
    echo "  <aws_config_file> is a file that contains AMI IDs, instance types, and security"
    echo "    groups that will be used for launching servers. See example_aws_config.sh."
    echo "  <custom_job_id> is a user-defined identifier for this run. It will be used in the"
    echo "    name of the EC2 instance, and for reporting. It is not used by the actual"
    echo "    Prometheus code, only the automation script."
    echo "  <sftp_file_name> should be the name of the input .zip file on the SFTP server"
    echo "  e.g. './run_prom.sh aws_config.sh 1234 50K_Test_2016_03_16.zip'"
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

JOB_ID="$2"
FILE_NAME="$3"
EC2_USER="ec2-user"
USER_HOME="/home/$EC2_USER"
ECR_HOME="/ecrfiles"
SFTP_FILE="/$USER_HOME/$FILE_NAME"

# SFTP configuration
SFTP_KEYFILE=$USER_HOME/.ssh/$KEY_NAME.pem
SFTP_USER="$EC2_USER"

# EC2 instance parameters
INSTANCE_NAME="PROM-$JOB_ID-$FILE_NAME"

LUIGI_DIR="$USER_HOME/payformance/luigi"

# output file and startup script file locations on worker servers
OUTPUT_DIR="$USER_HOME/prom_output_${JOB_ID}"
OUTPUT_FILE="$OUTPUT_DIR/$JOB_ID.log"

# base arguments for scp
# sudo is necessary because the script will run as root on startup
SFTP_COMMAND="sudo -u $EC2_USER scp -i $SFTP_KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# location and command for copying the input .zip file to the runner instance
DOWNLOAD_DIR="$ECR_HOME/input"
DOWNLOAD_FILE="$DOWNLOAD_DIR/$FILE_NAME"
DOWNLOAD_SFTP_FILE_PATH="$SFTP_FILE"
DOWNLOAD_COMMAND="$SFTP_COMMAND ${SFTP_USER}@${SFTP_SERVER}:$DOWNLOAD_SFTP_FILE_PATH $DOWNLOAD_FILE"

# location and command for copying output files back to to the file server
UPLOAD_FILE="$USER_HOME/$JOB_ID-$FILE_NAME-output.zip"
UPLOAD_SFTP_FILE_PATH="${SFTP_FILE}-output.zip"
UPLOAD_COMMAND="$SFTP_COMMAND $UPLOAD_FILE ${SFTP_USER}@${SFTP_SERVER}:$UPLOAD_SFTP_FILE_PATH"

# local directory that contains output from AWS instance launching commands
LAUNCH_COMMAND_FILE="$JOB_ID-launch-commands"
LAUNCH_COMMAND_DIR="/tmp/$LAUNCH_COMMAND_FILE"
LAUNCH_SCRIPT_FILE="$LAUNCH_COMMAND_DIR/$JOB_ID.sh"

# create output directory
mkdir $LAUNCH_COMMAND_DIR

# this script will be run as root after the EC2 instance is launched
cat <<EOF > "$LAUNCH_SCRIPT_FILE"
#!/bin/bash

$DOWNLOAD_COMMAND

# create output directory
sudo -u $EC2_USER mkdir $OUTPUT_DIR

unzip -d $DOWNLOAD_DIR $DOWNLOAD_FILE

echo "export HOSTNAME=PROM-$JOB_ID" >> $USER_HOME/.bashrc

# edit luigi.cfg to contain the new job ID and file location
sed -i 's/<JOB_ID>/$JOB_ID/;\
        s/<FILE_NAME>/$FILE_NAME/;\
        s/<SFTP_SERVER>/$SFTP_SERVER/;\
        s/<KEY_NAME>/$KEY_NAME/'\
    $LUIGI_DIR/luigi.cfg

sudo -u $EC2_USER $LUIGI_DIR/doit.sh > $OUTPUT_DIR/$JOB_ID-luigi.log 2>&1 

EOF


# launch the root instance
ROOT_LAUNCH_COMMAND=$(cat <<ROOT_LAUNCH_COMMAND
aws ec2 run-instances \
    --image-id $ROOT_AMI_ID \
    --count 1 \
    --instance-type "$ROOT_INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids $SECURITY_GROUPS \
    --subnet-id "$SUBNET_ID" \
    --user-data "file://$LAUNCH_SCRIPT_FILE" \
    --no-associate-public-ip-address \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME}]' \
> $LAUNCH_COMMAND_DIR/root_instance.launch
ROOT_LAUNCH_COMMAND
)

echo "$ROOT_LAUNCH_COMMAND"

eval "$ROOT_LAUNCH_COMMAND"

# copy launch information to the SFTP server
LAUNCH_UPLOAD_FILE=${LAUNCH_COMMAND_DIR}.zip
zip -r $LAUNCH_UPLOAD_FILE $LAUNCH_COMMAND_DIR
$SFTP_COMMAND $LAUNCH_UPLOAD_FILE ${SFTP_USER}@${SFTP_SERVER}:${SFTP_FILE}-${LAUNCH_COMMAND_FILE}.zip


