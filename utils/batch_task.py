from prefect import *
from prefect.utilities.tasks import defaults_from_attrs
import time
import yaml
from utils.batch_utils import BatchExecutor,BatchInstance

def load_config(file_path):
    with open(file_path) as f:
        loaded_config = yaml.safe_load(f)
    return loaded_config

class BatchTask(Task):
    def __init__(self, 
                batch_instance: BatchInstance=None,
                target_path: str =None,
                **kwargs
    ):
        self.batch_instance = batch_instance
        self.target_path = target_path
        super().__init__(**kwargs)
        return

    @defaults_from_attrs("batch_instance", "target_path")
    def run(self, 
            batch_instance: BatchInstance=None,
            target_path: str=None,
    ):
        # TODO add argument to python command
        job_config = load_config(target_path+"/run.yaml")
        job_config['job_name'] = self.name #task name
        job_config['memory_size'] = batch_instance.instance_config['memory'] 
        job_config['vcpu_num'] = batch_instance.instance_config['vcpus'] 
        job_config['cpu_num'] = batch_instance.instance_config['cpus'] 

        #submit job
        batch_executor = BatchExecutor(clusterConfigFile="/Users/qinjielin/Downloads/NWU/aws_ws/boto_cluster/results/test_cluster/cluster_info.yaml",
                                        debugLog=False)
        batch_executor.submit_job(job_config=job_config, log_output=True)

        return batch_executor
