##########################################
#
# START AWS-SPECIFIC CONFIGURATION
#
##########################################

# This section contains configuration variables that 
# will depend on your specific AWS setup, such as
# IP addresses and security group IDs. These must be
# be specified manually.

# Key-pair name used by all servers
KEY_NAME=KEY_PAIR

# SFTP/SCP instance parameters
SFTP_SERVER=172.1.1.1

# AMIs for servers
ROOT_AMI_ID="ami-abcdef12"
MYSQL_AMI_ID="ami-abcdef12"
MONGO_AMI_ID="ami-abcdef12"
NORMAN_AMI_ID="ami-abcdef12"
CONNIE_AMI_ID="ami-abcdef12"

# instance types that will be used for servers
ROOT_INSTANCE_TYPE="r3.8xlarge"
MYSQL_INSTANCE_TYPE="r3.8xlarge"
MONGO_INSTANCE_TYPE="r3.8xlarge"
NORMAN_INSTANCE_TYPE="r3.8xlarge"
CONNIE_INSTANCE_TYPE="r3.8xlarge"

# Security groups that servers will be in (space-separated)
SECURITY_GROUPS="sg-abcdef12 sg-abcdef12"

# internal subnet that servers will be placed in (must have a connection to external internet)
SUBNET_ID="subnet-abcdef12"

##########################################
#
# END AWS-SPECIFIC CONFIGURATION
#
##########################################