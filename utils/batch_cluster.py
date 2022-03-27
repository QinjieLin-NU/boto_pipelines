#!/usr/bin/env python3

import boto3
import json
import time
import os

class BatchCluster:

    def __init__(self, awsKeyID, awsAccessKey, batchClusterName, attachedClusterName, awsRegion='us-east-1',
                    debugLog=False):

        AWS_REGION = awsRegion
        KEY_ID = awsKeyID 
        ACCESS_KEY = awsAccessKey
        self.EC2_RESOURCE = boto3.resource('ec2', aws_access_key_id=KEY_ID,
                            aws_secret_access_key=ACCESS_KEY,
                            region_name=AWS_REGION)
        self.EC2_CLIENT = boto3.client('ec2', aws_access_key_id=KEY_ID,
                            aws_secret_access_key=ACCESS_KEY,
                            region_name=AWS_REGION)
        self.EFS_CLIENT = boto3.client('efs', aws_access_key_id=KEY_ID,
                            aws_secret_access_key=ACCESS_KEY,
                            region_name=AWS_REGION)
        self.IAM_CLIENT = boto3.client('iam',aws_access_key_id=KEY_ID,
                            aws_secret_access_key=ACCESS_KEY,
                            region_name=AWS_REGION) 
        self.IAM_RESOURCE = boto3.resource('iam', aws_access_key_id=KEY_ID,
                            aws_secret_access_key=ACCESS_KEY,
                            region_name=AWS_REGION)
        self.S3_RESOURCE= boto3.resource('s3', aws_access_key_id=KEY_ID,
                            aws_secret_access_key=ACCESS_KEY,
                            region_name=AWS_REGION)
        self.BATCH_CLIENT = boto3.client('batch',aws_access_key_id=KEY_ID,
                            aws_secret_access_key=ACCESS_KEY,
                            region_name=AWS_REGION) 
        self.LOG_CLIENT = boto3.client('logs',aws_access_key_id=KEY_ID,
                                    aws_secret_access_key=ACCESS_KEY,
                                    region_name=AWS_REGION) 
        self.cluster_name = batchClusterName
        self.attached_cluster_name = attachedClusterName
        
        self.ASSIGN_VPC = None
        self.DEBUG_LOG = debugLog
        return

    def create_cluster(self, cpu_num=256, vpc_id=None):
        if vpc_id is None:
            batch_vpc = self.find_cluster_vpc(self.attached_cluster_name)
            if batch_vpc is None:
                batch_vpc = self.find_default_vpc()
        else:
            batch_vpc = self.assign_vpc(vpc_id)
        self.batch_vpc = batch_vpc
        
        batch_efs_id = self.find_efs(self.attached_cluster_name, self.batch_vpc)
        self.batch_efs_id = batch_efs_id

        batch_role_name =  "batchRole" + self.cluster_name
        batch_profile_name = "batchRole" + self.cluster_name
        batch_role_response, batch_instance_profile = self.create_batch_role(batch_role_name,batch_profile_name)
        self.batch_role_response, self.batch_instance_profile = batch_role_response, batch_instance_profile

        batch_bucket_name = "batch-bucket-" + self.cluster_name
        batch_bucket_response = self.create_batch_bucket(batch_bucket_name)
        self.batch_bucket_response = batch_bucket_response

        batch_sg_name = "batch-sg" + self.cluster_name
        batch_sg = self.creat_batch_sg(batch_vpc, batch_sg_name)
        self.batch_sg = batch_sg

        batch_env_name = "batch-compenv-" + self.cluster_name
        batch_comenv_response = self.create_batch_compenv(env_name=batch_env_name, 
                                                    role_response=batch_role_response, 
                                                    batch_sg=batch_sg, batch_vpc=batch_vpc, 
                                                    min_cpu=0, max_cpu=cpu_num)
        self.batch_comenv_response = batch_comenv_response
        time.sleep(1)

        batch_jobq_name = "batch-jobq-" + self.cluster_name
        batch_jobq_response = self.create_jobque(jobque_name=batch_jobq_name, compenv_name=batch_env_name)
        self.batch_jobq_response = batch_jobq_response

        return


    def destroy_cluster(self):
        self.delete_jobq(self.batch_jobq_response)
        self.delete_compenv(self.batch_comenv_response)
        self.destroy_sg([self.batch_sg])
        self.destroy_batch_iam(self.batch_role_response, self.batch_instance_profile)
        self.delete_batch_bucket(self.batch_bucket_response)
        return

    def find_efs(self, attached_cluster_name, attached_vpc):
        target_vpc_id = attached_vpc.id
        efs_response = self.EFS_CLIENT.describe_file_systems()
        for _r in efs_response['FileSystems']:
            if attached_cluster_name in _r['Name']:
                mount_resp = self.EFS_CLIENT.describe_mount_targets(FileSystemId=_r['FileSystemId'],)
                for _m in mount_resp['MountTargets']:
                    if _m['VpcId'] == target_vpc_id:
                        target_filesystem_id = _r['FileSystemId']
                        self.debug_log("find available efs id:",_r['FileSystemId'])
                        return target_filesystem_id
        self.debug_log("not available efs in cluster, create by yourself...")
        return None
    
    def find_cluster_vpc(self, attached_cluster_name):
        vpc_resp = self.EC2_CLIENT.describe_vpcs()
        if len(vpc_resp['Vpcs']) == 0:
            vpc, _, _, _ = create_vpc("first-vpc")
            return vpc
        else:
            for _v in vpc_resp['Vpcs']:
                try:
                    for _k in _v['Tags']:
                        if attached_cluster_name in _k['Value']:
                            self.debug_log("find vpc id:",_v['VpcId'])
                            return self.EC2_RESOURCE.Vpc(_v['VpcId'])
                except:
                    continue
        self.debug_log("provide cluster name not exist...")
        return None


    
    def assign_vpc(self, vpc_id):
        vpc = self.EC2_RESOURCE.Vpc(vpc_id)
        self.ASSIGN_VPC = vpc
        self.debug_log("using input vpc:",vpc_id)
        return vpc

    def create_batch_role(self, role_name, profile_name):
        ROLE_NAME = role_name
        INSTANCE_PROFILE_NAME =  profile_name

        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": [
                            "ec2.amazonaws.com",
                            "lambda.amazonaws.com",
                            "ecs-tasks.amazonaws.com"
                        ]
                    },
                    "Action": "sts:AssumeRole"
                }
            ]   
        }

        response = self.IAM_CLIENT.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        instance_profile = self.IAM_RESOURCE.create_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME,Path='/')


        self.IAM_CLIENT.attach_role_policy(
            RoleName=response['Role']['RoleName'],
            PolicyArn='arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryFullAccess'
        )

        self.IAM_CLIENT.attach_role_policy(
            RoleName=response['Role']['RoleName'],
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess'
        )

        self.IAM_CLIENT.attach_role_policy(
            RoleName=response['Role']['RoleName'],
            PolicyArn='arn:aws:iam::aws:policy/CloudWatchFullAccess'
        )

        self.IAM_CLIENT.attach_role_policy(
            RoleName=response['Role']['RoleName'],
            PolicyArn='arn:aws:iam::aws:policy/AmazonEC2FullAccess'
        )
        # qinjie
        self.IAM_CLIENT.attach_role_policy(
            RoleName=response['Role']['RoleName'],
            PolicyArn='arn:aws:iam::aws:policy/AmazonSSMFullAccess'
        ) 
        self.IAM_CLIENT.attach_role_policy(
            RoleName=response['Role']['RoleName'],
            PolicyArn='arn:aws:iam::aws:policy/AmazonElasticFileSystemFullAccess'
        )
        self.IAM_CLIENT.attach_role_policy(
            RoleName=response['Role']['RoleName'],
            PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role'
        )
        
        
        instance_profile.add_role(RoleName=ROLE_NAME)

        self.debug_log(f"create batch role id: {response['Role']['RoleId']}")
        self.debug_log(f'create batch iam profile id: {instance_profile.instance_profile_id}')
        
        return response,instance_profile

    def create_batch_bucket(self, bucket_name): 
        BUCKET_NAME = bucket_name
        response = self.S3_RESOURCE.create_bucket(Bucket=BUCKET_NAME)
        self.debug_log("create batch bucker",bucket_name)
        return response

    def creat_batch_sg(self, batch_vpc, sg_name):
        VPC_ID = batch_vpc.id
        SG_NAME= sg_name
        GROUP_NAME = sg_name + "-group"

        security_group = self.EC2_RESOURCE.create_security_group(
            Description='Allow all inbound traffic',
            GroupName=GROUP_NAME,
            VpcId=VPC_ID,
            TagSpecifications=[
                {
                    'ResourceType': 'security-group',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': SG_NAME
                        },
                    ]
                },
            ],
        )

        security_group.authorize_ingress(
            CidrIp='0.0.0.0/0', #if only accessed by vpc, try the same CiDR with vpc creation
            FromPort=-1,
            ToPort=-1,
            IpProtocol='-1',
        )
        self.debug_log(f'Security Group {security_group.id} has been created')

        return security_group

    def create_jobque(self, jobque_name, compenv_name):
        JOBQUE_NAME = jobque_name
        COMPENV_NAME = compenv_name
        
        response = self.BATCH_CLIENT.create_job_queue(
            jobQueueName=JOBQUE_NAME,
            state='ENABLED',
            priority=1,
            computeEnvironmentOrder=[
                {
                    'order': 100,
                    'computeEnvironment': COMPENV_NAME
                },
            ],
        )
        
        resp_status = 'UPDATING'
        while True:
            resp = self.BATCH_CLIENT.describe_job_queues(jobQueues=[JOBQUE_NAME,])
            resp_status = resp['jobQueues'][0]['status']
            self.debug_log("Enabling Job Queues:",JOBQUE_NAME,end="\r")
            if resp_status=='VALID':
                break
            time.sleep(1)
        self.debug_log("Successfully Enabling Job Queues:",JOBQUE_NAME)
        
        self.debug_log("create batch job queue:",response['jobQueueName'])
        return response


    def create_jobdef(self, jobdef_name, 
                    memory_size=256, vcpu_num=16, 
                    entry_cmd='./run_job.sh',
                    image_id='223004288560.dkr.ecr.us-east-1.amazonaws.com/aws-batch-tutorial:latest',
                    file_system_id=None, efs_path="/",container_path="/usr/local/python_ws/results"):
        JOBDEF_NAME = jobdef_name
        ROLE_NAME = self.batch_role_response['Role']['RoleName']
        MEMORY_SIZE = memory_size
        CPU_NUM=vcpu_num
        ENTRY_CMD = entry_cmd
        IMAGE_ID = image_id
        Batch_Role = self.IAM_CLIENT.get_role(RoleName=ROLE_NAME)
        FILE_SYSTEM_ID = file_system_id
        
        if FILE_SYSTEM_ID is None:
            self.debug_log("not attaching efs...")
        else:
            if FILE_SYSTEM_ID ==1:
                FILE_SYSTEM_ID=self.batch_efs_id
                self.debug_log("attaching default efs id:",FILE_SYSTEM_ID)
            else:
                self.debug_log("attaching efs id:",FILE_SYSTEM_ID)

        response = self.BATCH_CLIENT.register_job_definition(
            jobDefinitionName=JOBDEF_NAME,
            type='container',
            containerProperties={
                'image': IMAGE_ID,
                'memory': MEMORY_SIZE,
                'vcpus': CPU_NUM,
                'jobRoleArn': Batch_Role['Role']['Arn'],
                'executionRoleArn': Batch_Role['Role']['Arn'],
                'volumes': [
                    {
                        'name': 'efsmount', #qinjie
                        'efsVolumeConfiguration': {
                            'fileSystemId': FILE_SYSTEM_ID,
                            'rootDirectory': efs_path,
                        }
                    },
                ],
                "mountPoints": [
                    {
                        "containerPath": container_path,
                        "sourceVolume": "efsmount"
                    }
                ],
                'environment': [
                    {
                        'name': 'AWS_DEFAULT_REGION',
                        'value': 'us-east-1',
                    }
                ],
                'command': ENTRY_CMD, #qinjie very important
            },
        )

        self.debug_log("create batch job definition:",response['jobDefinitionName'])

        jobdef_status = "None"
        while True:
            des_resp = self.BATCH_CLIENT.describe_job_definitions( jobDefinitions=[response['jobDefinitionArn'],],)
            jobdef_status = des_resp['jobDefinitions'][0]['status']
            self.debug_log("waiting for job definition to be active...")
            if jobdef_status=='ACTIVE':
                break
            time.sleep(5)

        return response

    def sub_job(self, job_name, jobdef_resp):
        JOB_NAME = job_name
        JOBDEF_NAME = jobdef_resp['jobDefinitionName']
        JOBQ_NAME = self.batch_jobq_response['jobQueueName']
        
        response = self.BATCH_CLIENT.submit_job(
            jobDefinition=JOBDEF_NAME,
            jobName=JOB_NAME,
            jobQueue=JOBQ_NAME,
            containerOverrides={
                'environment': [
                    {
                        'name': 'env_name',
                        'value': 'batch-test',
                    },
                ]
            },
        )
        self.debug_log("submit batch job:",response['jobName'])
        return response

    def create_vpc(self, vpc_name):
        # create VPC
        vpc = self.EC2_RESOURCE.create_vpc(CidrBlock='10.0.0.0/16')
        time.sleep(3) #vpc need time to be created
        vpc.create_tags(Tags=[{"Key": "NAME", "Value": vpc_name}])
        vpc.wait_until_available()
        self.debug_log("created vpc id:",vpc.id)

        self.EC2_CLIENT.modify_vpc_attribute( VpcId = vpc.id , EnableDnsSupport = { 'Value': True } )
        self.EC2_CLIENT.modify_vpc_attribute( VpcId = vpc.id , EnableDnsHostnames = { 'Value': True } )

        # create subnet
        subnet = self.EC2_RESOURCE.create_subnet(CidrBlock='10.0.0.0/16', VpcId=vpc.id)
        self.debug_log("created subnet id:",subnet.id)

        # create then attach internet gateway
        ig = self.EC2_RESOURCE.create_internet_gateway()
        time.sleep(3)
        vpc.attach_internet_gateway(InternetGatewayId=ig.id)
        self.debug_log("Internet Gateway id:",ig.id)

        # create a route table and a public route
        route_table = vpc.create_route_table()
        route = route_table.create_route(
            DestinationCidrBlock='0.0.0.0/0',
            GatewayId=ig.id
        )
        self.debug_log("Route Table id:",route_table.id)
        route_table.associate_with_subnet(SubnetId=subnet.id)

        return vpc, subnet, ig, route_table

    def find_default_vpc(self,):
        vpc_resp = self.EC2_CLIENT.describe_vpcs()
        if len(vpc_resp['Vpcs']) == 0:
            vpc, _, _, _ = create_vpc("first-vpc")
            return vpc
        else:
            for _v in vpc_resp['Vpcs']:
                if _v["IsDefault"]:
                    vpc = self.EC2_RESOURCE.Vpc(_v['VpcId'])
                    self.debug_log("found default vpc",vpc.id)
                    return vpc
        
        # if not found default vpc return the first one
        vpc = self.EC2_RESOURCE.Vpc(vpc_resp['Vpcs'][0]['VpcId'])
        self.debug_log("using first vpc:",vpc.id)
        return vpc
            
    def create_batch_compenv(self, env_name, role_response, batch_sg, batch_vpc, min_cpu=0, max_cpu=256):
        COMP_ENV_NAME = env_name
        INSTANCE_ROLE = role_response['Role']['RoleName'] #TODO
        SG_ID = batch_sg.id
        SUBNET_IDs = []
        MIN_CPU = min_cpu
        MAX_CPU = max_cpu
        self.debug_log("bound role:",INSTANCE_ROLE)

        vpc_res = self.EC2_CLIENT.describe_subnets( Filters=[{
                                        'Name': 'vpc-id',
                                        'Values': [batch_vpc.id,]
                                        },],)
        for sub_res in vpc_res['Subnets']:
            SUBNET_IDs.append( sub_res['SubnetId'] )
            # qinjie make sure subnet AutoAssignIPV4 is anabled and compenv has internet access
            self.EC2_CLIENT.modify_subnet_attribute(   
                                        MapPublicIpOnLaunch={'Value': True},
                                        SubnetId=sub_res['SubnetId'] ,) 

        response = self.BATCH_CLIENT.create_compute_environment(
            computeEnvironmentName=COMP_ENV_NAME,
            type='MANAGED',
            state='ENABLED',
            computeResources={
                'type': 'EC2', #'EC2',
                'allocationStrategy': 'BEST_FIT',
                'minvCpus': MIN_CPU,
                'maxvCpus': MAX_CPU,
                'subnets': SUBNET_IDs,
                'instanceRole': INSTANCE_ROLE, #"ecsInstanceRole", #INSTANCE_ROLE, qinjie
                'securityGroupIds': [SG_ID,], #add handed default sg qinjie, 
                'instanceTypes': ['optimal',],
                #'ec2KeyPair': "boto-ssh-key-pair", # qinjie try  without keyname
            }
        )
        self.debug_log("create batch computer environment:",response['computeEnvironmentName'])

        resp_status = 'UPDATING'
        while True:
            resp = self.BATCH_CLIENT.describe_compute_environments(computeEnvironments=[COMP_ENV_NAME,])
            resp_status = resp['computeEnvironments'][0]['status']
            self.debug_log("Enabling Computer Environment:",COMP_ENV_NAME,end="\r")
            if resp_status == 'VALID':
                break
            time.sleep(1)
        self.debug_log("Successfully Enabling Computer Environment:",COMP_ENV_NAME,)

        return response
    
    def destroy_job(self, job_response):
        job_id = job_response['jobId']
        response = self.BATCH_CLIENT.terminate_job(
            jobId=job_id,
            reason='Terminating job.',
        )
        response = self.BATCH_CLIENT.cancel_job(
            jobId=job_id,
            reason='cancel job'
        )

    def deregister_job_definition(self, jondef_response):
        job_def = jondef_response["jobDefinitionArn"]
        response = self.BATCH_CLIENT.deregister_job_definition(
            jobDefinition= job_def
        )
        self.debug_log("deregister job definition:",job_def)

    def delete_jobq(self, jobq_response):
        bjobq_name = jobq_response['jobQueueName']
        self.BATCH_CLIENT.update_job_queue(jobQueue=bjobq_name,state='DISABLED')
        
        resp_status = 'UPDATING'
        while resp_status!='VALID':
            resp = self.BATCH_CLIENT.describe_job_queues(
                                jobQueues=[bjobq_name,])
            resp_status = resp['jobQueues'][0]['status']
            self.debug_log("Disabling Job Queues:",bjobq_name, end="\r")
            time.sleep(5)
        self.debug_log("Successfully Disabling Job Queues:",bjobq_name)
            
        self.BATCH_CLIENT.delete_job_queue(jobQueue=bjobq_name)
        
        num=1
        while num>0:
            resp = self.BATCH_CLIENT.describe_job_queues(
                                jobQueues=[bjobq_name,])
            num = len(resp['jobQueues'])
            self.debug_log("Deleting Job Queues:",bjobq_name, end="\r")
            time.sleep(15)
        self.debug_log("Successfully Deleting Job Queues:",bjobq_name)
        
        return

    def delete_compenv(self, compenv_response):
        compenv_name = compenv_response['computeEnvironmentName']
        
        response = self.BATCH_CLIENT.update_compute_environment(
                                    computeEnvironment=compenv_name,
                                    state='DISABLED')
        
        resp_status = 'UPDATING'
        while resp_status!='VALID':
            resp = self.BATCH_CLIENT.describe_compute_environments(
                                computeEnvironments=[compenv_name,])
            resp_status = resp['computeEnvironments'][0]['status']
            self.debug_log("Disabling Computer Environment:",compenv_name, end="\r")
            time.sleep(5)
        self.debug_log("Successfully disabling Computer Environment:",compenv_name)
            
        response = self.BATCH_CLIENT.delete_compute_environment(
            computeEnvironment=compenv_name
        )

        num=1
        while num>0:
            resp = self.BATCH_CLIENT.describe_compute_environments(
                                computeEnvironments=[compenv_name,])
            num = len(resp['computeEnvironments'])
            self.debug_log("Deleting Computer Environment:",compenv_name,end="\r")
            time.sleep(5)
        self.debug_log("Successfully deleting Computer Environment:",compenv_name)
        return 

    def destroy_sg(self, security_groups):
        for _s in security_groups:
            SECURITY_GROUP_ID = _s.id
            try:
                security_group = self.EC2_RESOURCE.SecurityGroup(SECURITY_GROUP_ID)
                security_group.delete()
                self.debug_log(f'Security Group {SECURITY_GROUP_ID} has been deleted')
            except:
                self.debug_log(f'Unable to delete Security Group {SECURITY_GROUP_ID}, delete it in the aws console')


    def destroy_batch_iam(self, role_response, iam_profile):
        iam_role = self.IAM_RESOURCE.Role(role_response['Role']['RoleName'])
        iam_profile.remove_role(RoleName=iam_role.role_name)
        self.IAM_CLIENT.delete_instance_profile(InstanceProfileName=iam_profile.name)
        self.debug_log("delete IAM profile:",iam_profile.name)
        
        resp = self.IAM_CLIENT.list_attached_role_policies(RoleName=iam_role.name)
        for _p in resp['AttachedPolicies']:
            _arn = _p['PolicyArn']
            _resp = iam_role.detach_policy(PolicyArn=_arn)
        self.IAM_CLIENT.delete_role(RoleName=iam_role.name)
        self.debug_log("delete IAM role:",iam_role.name)

    def delete_batch_bucket(self, input_bucket):
        responce = input_bucket.delete()
        self.debug_log("delete bucket",input_bucket.name)
        return
    
    def log_job(self, job_resp):
        response = self.BATCH_CLIENT.describe_jobs(jobs=[job_resp['jobId'],])
        count = 0
        while True:
            attempt_num = len(response['jobs'][0]['attempts'])
            if attempt_num > 0:
                break
            status = response['jobs'][0]['status']
            if status =='FAILED':
                break
            print("waiting %is, current status:"%count, status, end="\r")
            time.sleep(2)
            response = self.BATCH_CLIENT.describe_jobs(jobs=[job_resp['jobId'],])
            count+=2
        print("job start after %is ......."%count)
        print("="*20)

        log_stream_name = "None"
        for _b in response['jobs']:
            try:
                log_stream_name = response['jobs'][0]['attempts'][-1]['container']['logStreamName']
            except:
                self.debug_log("no log stream...")
                break

        try:
            log_response = self.LOG_CLIENT.get_log_events(
                logGroupName='/aws/batch/job',
                logStreamName=log_stream_name,
            )
            for _e in log_response['events']:
                print(_e['message'])        
        except:
            response = self.BATCH_CLIENT.describe_jobs(jobs=[job_resp['jobId'],])
            self.debug_log(response)

        job_response = self.BATCH_CLIENT.describe_jobs(jobs=[job_resp['jobId'],])
        print("="*20)
        print("job running status:", job_response['jobs'][-1]['status'])

    def debug_log(self, *args,end="\n"):
        if self.DEBUG_LOG:
            print(*args,end=end)
        else:
            return

    def create_jobdef_v2(self, jobdef_name, 
                    memory_size=256, vcpu_num=16, 
                    entry_cmd='./run_job.sh',
                    image_id='223004288560.dkr.ecr.us-east-1.amazonaws.com/aws-batch-tutorial:latest',
                    volumes=[], mount_points=[]):
        JOBDEF_NAME = jobdef_name
        ROLE_NAME = self.batch_role_response['Role']['RoleName']
        MEMORY_SIZE = memory_size
        CPU_NUM=vcpu_num
        ENTRY_CMD = entry_cmd
        IMAGE_ID = image_id
        Batch_Role = self.IAM_CLIENT.get_role(RoleName=ROLE_NAME)

        response = self.BATCH_CLIENT.register_job_definition(
            jobDefinitionName=JOBDEF_NAME,
            type='container',
            containerProperties={
                'image': IMAGE_ID,
                'memory': MEMORY_SIZE,
                'vcpus': CPU_NUM,
                'jobRoleArn': Batch_Role['Role']['Arn'],
                'executionRoleArn': Batch_Role['Role']['Arn'],
                'volumes': volumes,
                "mountPoints": mount_points,
                'environment': [
                    {
                        'name': 'AWS_DEFAULT_REGION',
                        'value': 'us-east-1',
                    }
                ],
                'command': ENTRY_CMD, #qinjie very important
            },
        )

        self.debug_log("create batch job definition:",response['jobDefinitionName'])

        jobdef_status = "None"
        while True:
            des_resp = self.BATCH_CLIENT.describe_job_definitions( jobDefinitions=[response['jobDefinitionArn'],],)
            jobdef_status = des_resp['jobDefinitions'][0]['status']
            self.debug_log("waiting for job definition to be active...")
            if jobdef_status=='ACTIVE':
                break
            time.sleep(5)

        return response