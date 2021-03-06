import configparser
import boto3
from botocore.exceptions import ClientError
import json
import pandas as pd

def connect_iam_redshift(config):
    """
    Create connection sessions from iam/redshift
    in AWS and return the session object.
    :param config: configparser object
    :return: iam roles, redshift object.
    """
    iam = boto3.client('iam',aws_access_key_id=config.get('AWS','KEY'),
                     aws_secret_access_key=config.get('AWS','SECRET'),
                     region_name='us-west-2'
                  )
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=config.get('AWS','KEY'),
                       aws_secret_access_key=config.get('AWS','SECRET')
                       )
    return iam,redshift
    
def create_iam(config,iam):
    """
    Create iam roles if not exists
    and grant redshift access permission.
    :param config: configparser object
    :param iam: iam roles
    :return: iam role credential
    """
    iam_role_name = config.get('DWH','DWH_IAM_ROLE_NAME')
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
                                    Path='/',
                                    RoleName=iam_role_name,
                                    Description = "Allows Redshift clusters to call AWS services on your behalf.",
                                    AssumeRolePolicyDocument=json.dumps(
                                                                        {'Statement': [
                                                                                        {
                                                                                          'Action': 'sts:AssumeRole',
                                                                                          'Effect': 'Allow',
                                                                         'Principal': {'Service': 'redshift.amazonaws.com'}
                                                                                        }
                                                                                      ],
                                                                         'Version': '2012-10-17'
                                                                        }
                                                                       )
                                  )    
    except Exception as e:
        print(e)
    
    
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=iam_role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    print (iam.get_role(RoleName=iam_role_name)['Role']['Arn'])
    return iam.get_role(RoleName=iam_role_name)['Role']['Arn']


    
def create_cluster(config,iam_role,redshift):    
    """
        Create redshift cluster if not exists.
    """
    try:
        response = redshift.create_cluster(        
            ClusterType=config.get('DWH','DWH_CLUSTER_TYPE'),
            NodeType=config.get('DWH','DWH_NODE_TYPE'),
            NumberOfNodes=int(config.get('DWH','DWH_NUM_NODES')),
            DBName=config.get('CLUSTER','DB_NAME'),
            ClusterIdentifier=config.get('DWH','DWH_CLUSTER_IDENTIFIER'),
            MasterUsername=config.get('CLUSTER','DB_USER'),
            MasterUserPassword=config.get('CLUSTER','DB_PASSWORD'),
            IamRoles=[iam_role]
        )
    except Exception as e:
        print(e)
    #dwhAttribute = getClusterAttribute(redshift,config)
    #return dwhAttribute
    
        

def prettyRedshiftProps(props):
    """
        Return the status of cluster as pandas data frame
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def getClusterAttribute(redshift,config):
    """
        Get redshift description attribute.
    """
    DWH_CLUSTER_IDENTIFIER=config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    myClusterProps=redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    #DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    return prettyRedshiftProps(myClusterProps)
           
        
def cleanup_cluster(config,iam,redshift):
    """
        Drop clusters.
    """
    clusterident=config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    DWH_IAM_ROLE_NAME = config.get('DWH','DWH_IAM_ROLE_NAME')
    redshift.delete_cluster( ClusterIdentifier=clusterident,  SkipFinalClusterSnapshot=True)
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    
    iam,redshift = connect_iam_redshift(config)
    iam_role = create_iam(config,iam)
    #create_cluster(config,iam_role,redshift)

    dwhattr=getClusterAttribute(redshift,config)
    print ("IAM: {}".format(iam_role)+","+str(dwhattr))
   
    """execute the following drop command if cluster no longer needed."""
    #cleanup_cluster(config,iam,redshift)
   


    
if __name__ == "__main__":
    main()