import time
from utils.batch_cluster import BatchCluster
import uuid
import yaml
def load_config(file_path):
    with open(file_path) as f:
        loaded_config = yaml.safe_load(f)
    return loaded_config

class BatchInstance():
    """
    creare computer env and job queue
    """
    def __init__(self, 
                clusterConfigFile="../../.cluster/cluster.config", 
                debugLog=False,
                cpus=15,
                vcpus=15, 
                gpus=0, 
                memory=60
    ):
        self.cluster_config = load_config(clusterConfigFile)
        self.batch_clustername = "batchjob" +str(uuid.uuid4())[0:4]
        self.batch_cluster = BatchCluster(awsKeyID=self.cluster_config['aws_access_key_id'], 
                                        awsAccessKey=self.cluster_config['aws_secret_access_key'],
                                        batchClusterName=self.batch_clustername, 
                                        attachedClusterName=self.cluster_config['cluster_name'],
                                        debugLog=debugLog)
        self.instance_config = {
            "cpus": cpus,
            "vcpus": vcpus,
            "gpus": gpus,
            "memory": memory,
        }
        self.create_compenv(cpus)
        return
    
    def create_compenv(self,cpus):
        #TODO add gpu and memory option
        print("create computer env")  
        vpc_id = self.cluster_config['vpc_id']
        self.batch_cluster.create_cluster(cpu_num=cpus, vpc_id=vpc_id)      
        return

    def delete_compenv(self):
        #delete cluster
        self.batch_cluster.destroy_cluster()
        return

class BatchExecutor():
    """
    submit job to the computer_env
    """
    def __init__(self, **kwargs):
        return
    
    def submit_job(self, job_config=None, batch_instance=None, log_output=True):
        print("submitting job")
        self.batchjob_config = job_config
        self.batch_cluster = batch_instance.batch_cluster
        self.batch_clustername = batch_instance.batch_clustername
        self.cluster_config = batch_instance.cluster_config

        # comp_cpu_num = self.batchjob_config['cpu_num'] 
        # The hard limit (in GB) of memory to present to the container.
        memory_size = self.batchjob_config['memory_size']
        # The number of vCPUs reserved for the container. Each vCPU is equivalent to 1,024 CPU shares.
        vcpu_num = self.batchjob_config['vcpu_num'] 
        entry_cmd = self.batchjob_config['entry_cmd'] 
        image_name = self.batchjob_config['image_name'] 
        job_name = self.batchjob_config['job_name'] 
        mount_configs = self.batchjob_config['mount_config']

        #generate mount config
        volumes = []
        mount_points = []
        mount_id = 0
        for mount_config in mount_configs:
            file_id = self.find_efs(mount_config['efs_name'])
            efs_path = mount_config['efs_path']
            container_path = mount_config['container_path']
            _v={
                'name': f'efsmount{mount_id}',
                'efsVolumeConfiguration': {
                    'fileSystemId': file_id,
                    'rootDirectory': efs_path,
                    }
                }
            _m={
                "containerPath": container_path,
                "sourceVolume": f'efsmount{mount_id}'
                }
            volumes.append(_v)
            mount_points.append(_m)
            mount_id+=1

        # submit job
        batch_jobdef_name = "jobdef-" + self.batch_clustername + "-" + job_name 
        batch_jobdef_response = self.batch_cluster.create_jobdef_v2(jobdef_name=batch_jobdef_name, 
                                        memory_size=memory_size, vcpu_num=vcpu_num, 
                                        entry_cmd=entry_cmd,
                                        image_id=image_name,
                                        volumes=volumes,
                                        mount_points=mount_points)
        batch_job_name = "job-" + self.batch_clustername + "-" + job_name 
        batch_job_response = self.batch_cluster.sub_job(job_name=batch_job_name, jobdef_resp=batch_jobdef_response)
        
        # log job
        if log_output:
            self.batch_cluster.log_job(batch_job_response)
        
        print("deleting job...")
        # delete job
        self.batch_cluster.deregister_job_definition(batch_jobdef_response)
        self.batch_cluster.destroy_job(batch_job_response)

        return batch_job_response

    def find_efs(self, efs_name):
        efs_id = None
        for _d in self.cluster_config['efs_dict']:
            if _d['name'] == efs_name:
                efs_id = _d['efs_id']
                return efs_id
        print("not finding efs corresponding efs_name")
        return None