# -*- coding: UTF-8 -*-
# @author =__Uluç Furkan Vardar__
import boto3
import time
import json
from datetime import datetime

# --- EC2_Controller ---------------------------------------------------------------------------
class EC2_controller:
    def __init__(self,ImageId = None, InstanceType = None , IamInstanceProfile = None , instance_id = None):
        self.ImageId = ImageId
        self.instance_id = instance_id
        self.InstanceType = InstanceType
        self.IamInstanceProfile = IamInstanceProfile
        self.client = boto3.client('ec2')
        self.ssm_client = boto3.client('ssm') 

    #------------- EC2 Create--------------
    def create_an_Ec2_and_configure(self):
        def state_look(id):
            ec2 = boto3.resource('ec2')
            for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
                if status['InstanceId'] == id :
                    return status['SystemStatus']['Status']
            return 'None'
        resp = self.client.run_instances( ImageId = self.ImageId ,
                                    InstanceType = self.InstanceType, #m5a.xlarge
                                    MinCount = 1,
                                    MaxCount = 1,
                                    IamInstanceProfile={
                                        'Arn': self.IamInstanceProfile
                                    })
        for instance in resp['Instances']:
            instance_id =  instance['InstanceId']
            break
        flag = True
        i = 0
        while flag == True:
            
            ###----
            global start_time
            c = datetime.now()-start_time
            dif = int(divmod(c.days * 86400 + c.seconds, 60)[0])
            print "Başlayalı ",dif,"dk oldu."
            if 14 <= dif :
                print  "---------------------KOD BAŞLAYALI 14 DK OLDU SISTEM KAPANACAK---------------------"
                
                
                
                print '----- Ec2 kapatilacak -----'
                global mss_id
                flag = self.terminate_an_Ec2()
                sendTelegramMess (header = {
        							"chat_id": "-387770577",
        							'from' : '[Lambda] Hurriyet_Recomadation, id : '+instance_id, #bold
        							'text' :  'EC2 CLOSED!',
        							'reply_to_message_id' : mss_id
        							})               
                print '----- Ec2 Kapatılma durumu ',flag,'-----'  
            ####---
            
            state = state_look(instance_id)
            #print "iterate",i,state
            i +=1
            #flag = False ## deneme
            #if state == "initializing":
            if state == 'ok':
                flag = False
                break
            else:
                time.sleep(2)   
        time.sleep(5)   
        self.instance_id = instance_id
        return instance_id
    #--------------------------------------
    
    #------------- EC2 Terminate-----------
    def terminate_an_Ec2(self):
        time.sleep(2) # wait for a certain time
        try:
            response = self.client.terminate_instances(  InstanceIds=[ self.instance_id ],
                                                    DryRun=False )
            return True
        except Exception as e:
            raise Exception ("[EC2 Terminate ERROR]: "+str(e))
    #--------------------------------------
    
    
    #------------EC2 Run Script -----------
    def run_script_fromS3(self, path , command):
        print type(self.instance_id)
        print  [self.instance_id],'brda'
        print  type([self.instance_id]),'brda'
        
        instance_ids = list()
        instance_ids.append(self.instance_id)
        resp = self.ssm_client.send_command(
            DocumentName="AWS-RunRemoteScript",
            Parameters={
                        "sourceType":  ["S3"],
                        "sourceInfo":  [path],
                        "commandLine": [command]
                        },
                        InstanceIds = instance_ids
            )


    def run_command(self,commands, OutputS3BucketName_ = None, OutputS3KeyPrefix_ = None):

        if OutputS3BucketName_ == None and OutputS3KeyPrefix_ == None:
            response = self.ssm_client.send_command(
                                                DocumentName = "AWS-RunShellScript",
                                                Parameters   = {'commands': commands },
                                                InstanceIds  = [self.instance_id]     )
        else:
            response = self.ssm_client.send_command(
                                                OutputS3BucketName = OutputS3BucketName_,
                                                OutputS3KeyPrefix = OutputS3KeyPrefix_  ,
                                                DocumentName = "AWS-RunShellScript"     ,
                                                Parameters   = {'commands': commands }  ,
                                                InstanceIds  = [self.instance_id]     )

        print response
    #--------------------------------------
    
    def set_image_id(self, ImageId):
        self.ImageId = ImageId
    def set_instance_id(self,instance_id):
        self.instance_id = instance_id
# ----------------------------------------------------------------------------------------------

    
### USE EXAMPLES 

def ec2_create_example():
    ec2_c = EC2_controller( ImageId = 'ami-...........', # Ec2 image ID 
                            InstanceType = 't2.large' ,  # 'm5a.xlarge'
                            IamInstanceProfile = '.......' ) # EC2 ınstance role
    ## next 2 line lunch an ec2 with setted conf. #code waits until machine access to ready state. Took a while (3-10 min)
    instance_id = ec2_c.create_an_Ec2_and_configure()
    print 'instance ID',instance_id


def ec2_runcommand_example():
    '''
    after creation of ec2 or an exist ec2
    '''
    ec2_c = EC2_controller( instance_id = '...id..of..instance...' ) # EC2 ınstance role
    commands = ["touch /home/ec2-user/examplefile.txt"]          
    ec2_c.run_command(commands)
        
def ec2_terminate_example():
    #to  terminate an ec2
    ec2_c = EC2_controller()
    ec2_c.set_instance_id(".....instance_id....")

    flag = ec2_c.terminate_an_Ec2()        
