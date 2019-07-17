# -*- coding: UTF-8 -*-
# @author =__Uluç Furkan Vardar__
import boto3
import time
import json
from datetime import datetime

# --- EC2_Controller ---------------------------------------------------------------------------
class EC2_controller:
    def __init__(self,ImageId = None, InstanceType = None , IamInstanceProfile = None , instance_id = None, instance_ids = list(), number_of_nodes = 1, instance_name = 'Ec2_Controller_Class_Configured_Ec2'):
        self.number_of_nodes = number_of_nodes 
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

    
