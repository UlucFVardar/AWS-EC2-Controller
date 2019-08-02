# -*- coding: UTF-8 -*-
# @author =__Uluç Furkan Vardar__
import boto3
import time
import json
from datetime import datetime
import os
# --- EC2_Controller ---------------------------------------------------------------------------
class EC2_controller:
    def __init__(self,ImageId = None, InstanceType = None , IamInstanceProfile = None , instance_id = None, instance_ids = None, number_of_nodes = 1, instance_name = 'Ec2_Controller_Class_Configured_Ec2'):
        self.number_of_nodes = number_of_nodes 
        if instance_ids != None:
            self.instance_ids = instance_ids
        self.ImageId = ImageId
        self.instance_id = instance_id
        self.InstanceType = InstanceType
        self.IamInstanceProfile = IamInstanceProfile
        self.client = boto3.client('ec2')
        self.ssm_client = boto3.client('ssm') 
        self.instance_name = instance_name

    #------------- EC2 Create--------------
    def start_ec2s(self, Tag_Key, Tag_Value, key_pair_name = None, SecurityGroup = None ):
        if SecurityGroup != None:
            if key_pair_name == None:
                resp = self.client.run_instances( ImageId = self.ImageId ,                            
                                                  InstanceType = self.InstanceType, 
                                                  MinCount = self.number_of_nodes ,
                                                  MaxCount = self.number_of_nodes ,
                                                  IamInstanceProfile={ 'Arn': self.IamInstanceProfile },
                                                  SecurityGroups=[SecurityGroup],
                                                  TagSpecifications=[
                                                                    {
                                                                                    'ResourceType': 'instance',
                                                                        'Tags': [
                                                                            {
                                                                                'Key': 'Name',
                                                                                'Value': self.instance_name
                                                                            },{
                                                                                'Key': Tag_Key,
                                                                                'Value': Tag_Value
                                                                            },
                                                                        ]
                                                                    },
                                                                ],
                                                                     )   
            else:
                resp = self.client.run_instances( ImageId = self.ImageId ,                                                                          
                                                  KeyName  = key_pair_name,
                                                  MinCount = self.number_of_nodes ,
                                                  MaxCount = self.number_of_nodes ,
                                                  SecurityGroups=[SecurityGroup],
                                                  InstanceType = self.InstanceType,                                               
                                                  IamInstanceProfile={ 'Arn': self.IamInstanceProfile },
                                                  TagSpecifications=[
                                                                    {
                                                                                    'ResourceType': 'instance',
                                                                        'Tags': [
                                                                            {
                                                                                'Key': 'Name',
                                                                                'Value': self.instance_name
                                                                            },{
                                                                                'Key': Tag_Key,
                                                                                'Value': Tag_Value
                                                                            },
                                                                        ]
                                                                    },
                                                                ],
                                                                     )               
        else:
            if key_pair_name == None:
                resp = self.client.run_instances( ImageId = self.ImageId ,                            
                                                  InstanceType = self.InstanceType, 
                                                  MinCount = self.number_of_nodes ,
                                                  MaxCount = self.number_of_nodes ,
                                                  IamInstanceProfile={ 'Arn': self.IamInstanceProfile },
                                                  TagSpecifications=[
                                                                    {
                                                                                    'ResourceType': 'instance',
                                                                        'Tags': [
                                                                            {
                                                                                'Key': 'Name',
                                                                                'Value': self.instance_name
                                                                            },
                                                                        ]
                                                                    },
                                                                ],
                                                                     )   
            else:
                resp = self.client.run_instances( ImageId = self.ImageId ,                                                                          
                                                  KeyName  = key_pair_name,
                                                  MinCount = self.number_of_nodes ,
                                                  MaxCount = self.number_of_nodes ,
                                                  InstanceType = self.InstanceType,                                               
                                                  IamInstanceProfile={ 'Arn': self.IamInstanceProfile },
                                                  TagSpecifications=[
                                                                    {
                                                                                    'ResourceType': 'instance',
                                                                        'Tags': [
                                                                            {
                                                                                'Key': 'Name',
                                                                                'Value': self.instance_name
                                                                            },
                                                                        ]
                                                                    },
                                                                ],
                                                                     )               



        self.instance_ids = []
        for instance in resp['Instances']:
            instance_id =  str(instance['InstanceId'])
            
            self.instance_ids.append( instance_id )
        return self.instance_ids
    #--------------------------------------

    #------------- EC2 Terminate-----------
    def terminate_ec2s(self):
        if self.number_of_nodes > 1:
            instance_list = self.instance_ids
        elif self.number_of_nodes == 1:
            instance_list = [self.instance_id]

        time.sleep(2) # wait for a certain time
        try:
            response = self.client.terminate_instances(  InstanceIds = instance_list, DryRun=False )
            return True
        except Exception as e:
            raise Exception ("[EC2 Terminate ERROR]: "+str(e))
    #--------------------------------------
    
  
    #------------- EC2 Code Run -----------
    def run_command(self, commands, workingDirectory, OutputS3BucketName_ = None, OutputS3KeyPrefix_ = "Ec2_Controller_Class_Output", executionTimeout = "172800", instance_id = None ):
        if self.number_of_nodes > 1:
            instance_list = self.instance_ids
        elif self.number_of_nodes == 1:
            instance_list = [self.instance_id]
        if instance_id !=None:
            instance_list = [instance_id]

        if OutputS3BucketName_ == None and OutputS3KeyPrefix_ == None:
            response = self.ssm_client.send_command(
                                                DocumentName = "AWS-RunShellScript"                   ,
                                                InstanceIds  = instance_list                          ,
                                                Parameters   = { "commands": commands                 ,
                                                                 "workingDirectory":[workingDirectory],
                                                                 "executionTimeout":[executionTimeout] }  )
        else:
            response = self.ssm_client.send_command(
                                                OutputS3BucketName = OutputS3BucketName_              ,
                                                OutputS3KeyPrefix  = OutputS3KeyPrefix_               ,
                                                DocumentName = "AWS-RunShellScript"                   ,                                                
                                                InstanceIds  = instance_list                          ,
                                                Parameters   = { "commands": commands                 ,
                                                                 "workingDirectory":[workingDirectory],
                                                                 "executionTimeout":[executionTimeout] }  )

        print response
    #--------------------------------------

    def configure_machines(self):
        def state_look(id):
            ec2 = boto3.resource('ec2')
            for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
                if status['InstanceId'] == id :
                    return status['SystemStatus']['Status']
            return 'None'           
        #self.instance_ids
        self.flags = [False for number in range(self.number_of_nodes)]
        start_time = datetime.now()
        while True:
            ###----
            temp_dif = datetime.now()-start_time
            dif = int(divmod(temp_dif.days * 86400 + temp_dif.seconds, 60)[0])
            print "Başlayalı %s dk oldu."%str(dif)
            if 14 <= dif :
                print  "---------------------KOD BAŞLAYALI 14 DK OLDU SISTEM KAPANACAK---------------------"
                #CLOSE SYSTEM
            else:
                for i,instance_id in enumerate(self.instance_ids):
                    if self.flags[i] == True:
                        continue 
                    state = state_look(instance_id)    
                    if state == 'ok':
                        self.flags[i] = True
                #Wait a bit 
                time.sleep(2) 
            if False not in self.flags:
                break
        print "All EC2 Machines are ready!! "
        # Now here 

    def set_image_id(self, ImageId):
        self.ImageId = ImageId
    def set_instance_id(self,instance_id):
        self.instance_id = instance_id
    def set_instances_ids(self,instance_ids):
        self.instance_ids = instance_ids
# ----------------------------------------------------------------------------------------------
def close_your_self(region = 'eu-west-1'):
    instance_id = os.popen("curl http://169.254.169.254/latest/meta-data/instance-id").read()
    ec2 = boto3.resource('ec2',region)
    ec2.instances.filter(InstanceIds=[instance_id]).terminate()

def stop_your_self(region = 'eu-west-1'):
    instance_id = os.popen("curl http://169.254.169.254/latest/meta-data/instance-id").read()
    client = boto3.client('ec2',region)
    client.stop_instances(  InstanceIds = [instance_id], DryRun=False )    

    instances = ec2.instances.filter(Filters = [{'Name': 'tag:%s'%tag_name, 'Values': [tag_name]}])


# ==================================================================================================
# ======= Example ==================================================================================
# ==================================================================================================
def example():
    # ------- Create Ec2s -----------------------------
    number_of_slave = 1
    ec2s_c_Search = EC2_controller( ImageId             = 'ami-071341f40ec9....',  # Ec2 image ID 
                                    InstanceType        = 't2.micro' ,                # 'm5a.xlarge'
                                    IamInstanceProfile  = 'arn:aws:iam::027534141241.....',
                                    number_of_nodes     =  number_of_slave,
                                    instance_name       = 'Search Twint_Daily D : ..date..' ) # EC2 ınstance role

                            
    ## next 2 line lunch an ec2 with setted conf. #code waits until machine access to ready state. Took a while (3-10 min)
    ec2s_c_Search.start_ec2s( Tag_Key = 'Twint', Tag_Value = 'Uluc',key_pair_name = "ULUC_", SecurityGroup = "launch-wizard-18")
    print 'Search', ec2s_c_Search.instance_ids
    # -------------------------------------------------    # -------------------------------------------------
    ec2s_c_Search.configure_machines()
    

    # - ---- - - - -- - - OR
    for i in range(0,number_of_slave):   
        ec2s_c_Search.run_command(instance_id         = ec2s_c_Search.instance_ids[i]                    ,
                                  workingDirectory    = "/home/ec2-user/"                         ,
                                  OutputS3KeyPrefix_  = "Outputs/Twint Search D: %s -- %s"%(today_date, str(ec2s_c_Search.instance_ids[i])),
                                  OutputS3BucketName_ = "twitter-twint-crawled-data"              ,
                                  commands            = [ "export LANG=tr_TR.UTF-8"  ,
                                                          "aws s3 cp s3://twitter-twint-crawled-data/Scripts/userlists-for-slaves/Search/userL-%s.txt ./userData.txt"%str(i+1),
                                                          "aws s3 cp s3://twitter-twint-crawled-data/Scripts/searchData.zip ./searchData.zip",
                                                          "unzip searchData.zip"                    ,
                                                          "rm searchData.zip"                       ,
                                                          "python3 searchData.py"]     )
    # - ---- - - - -- - - OR
    # ALL OF THEM RUNS SAME CODE
    ec2s_c_Search.run_command(workingDirectory    = "/home/ec2-user/"                         ,
                              OutputS3KeyPrefix_  = "Outputs/Twint Search D: ALL OF THEM RUNS SAME CODE",
                              OutputS3BucketName_ = "twitter-twint-crawled-data"              ,
                              commands            = [ "export LANG=tr_TR.UTF-8"  ,
                                                      "aws s3 cp s3://twitter-twint-crawled-data/Scripts/userlists-for-slaves/Search/userL.txt ./userData.txt",
                                                      "aws s3 cp s3://twitter-twint-crawled-data/Scripts/searchData.zip ./searchData.zip",
                                                      "unzip searchData.zip"                    ,
                                                      "rm searchData.zip"                       ,
                                                      "python3 searchData.py"]     )    








