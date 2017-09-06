#!/bin/bash

# first: set up a user and group (go to IAM in the AWS console) http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
# The group should have full admin privileges

# instructions on running a script on startup: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/user-data.html

# check for arguments
# <custom_job_id> is a user-defined identifier for this run. It will be used in the name of the EC2 instance, and for reporting. It is not used by the actual Prometheus code, only the automation script.
# <file_name> should be the name of the input .zip file, but without the extension
# e.g. "./run_prom.sh 1234 50K_Test_2016_03_16"
if [ $# -lt 2 ]; then
    echo "Usage: run_prom.sh <custom_job_id> <file_name>"
    exit
fi

# FILE_NAME should omit the .zip extension
JOB_ID="$1"
FILE_NAME="$2"
SCP_FILE_PATH="/home/$EC2_USER/$FILE_NAME.zip"


# SCP configuration
KEY_NAME=PFS
EC2_USER="ec2-user"
SCP_KEYFILE=/home/$EC2_USER/.ssh/$KEY_NAME.pem
SCP_USER="$EC2_USER"
SCP_SERVER=172.31.1.203

# EC2 instance parameters
AMI_ID="ami-41f40339"
INSTANCE_TYPE="r3.8xlarge"
SECURITY_GROUP="sg-26f7c85c"
SUBNET_ID="subnet-1347774b"
INSTANCE_NAME="PROM-$JOB_ID-$FILE_NAME"

LUIGI_DIR="/home/$EC2_USER/payformance/luigi"

# output file and startup script file locations
OUTPUT_DIR="/home/$EC2_USER/prom_output"
OUTPUT_FILE="$OUTPUT_DIR/$JOB_ID.log"
SCRIPT_FILE="$OUTPUT_DIR/$JOB_ID.sh"

# location and command for copying the input .zip file to the runner instance
DOWNLOAD_DIR="/ecrfiles/input"
DOWNLOAD_FILE="$DOWNLOAD_DIR/$FILE_NAME.zip"
DOWNLOAD_COMMAND="sudo -u $EC2_USER scp -i $SCP_KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${SCP_USER}@${SCP_SERVER}:$SCP_FILE_PATH $DOWNLOAD_FILE"


# this script will be run as root after the EC2 instance is launched
cat <<EOF > "$SCRIPT_FILE"
#!/bin/bash

$DOWNLOAD_COMMAND

unzip -d $DOWNLOAD_DIR $DOWNLOAD_FILE

echo "export HOSTNAME=PROM-$JOB_ID" >> /home/$EC2_USER/.bashrc

# edit luigi.cfg to contain the new job ID and file location
sed -i 's/<JOB_ID>/$JOB_ID/; s/<FILE_NAME>/$FILE_NAME/' $LUIGI_DIR/luigi.cfg

sudo -u $EC2_USER $LUIGI_DIR/doit.sh > $LUIGI_DIR/$JOB_ID.log 2>&1 

EOF

# launch the instance
AWS_COMMAND=$(cat <<AWS_COMMAND
aws ec2 run-instances \
    --image-id $AMI_ID \
    --count 1 \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SECURITY_GROUP" \
    --subnet-id "$SUBNET_ID" \
    --user-data "file://$SCRIPT_FILE" \
    --no-associate-public-ip-address \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME}]'
AWS_COMMAND
)

echo "$AWS_COMMAND"

eval "$AWS_COMMAND"

