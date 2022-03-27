import csv 
import datetime

from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from utils.batch_task import BatchTask
from utils.batch_utils import BatchExecutor,BatchInstance

#batch computer env specification
batch_light = BatchInstance(cpus=15, vcpus=1, gpus=0, memory=60) 
batch_medium = BatchInstance(cpus=31, vcpus=1, gpus=0, memory=124)
batch_heavy = BatchInstance(cpus=63, vcpus=1, gpus=0, memory=126)

task1 = BatchTask(batch_instance=batch_light, target_path="./tasks/preprocess", name="preprocess")
task2 = BatchTask(batch_instance=batch_medium, target_path="./tasks/train", name="train")
task3 = BatchTask(batch_instance=batch_heavy, target_path="./tasks/inference", name="inference")


def build_flow():
    with Flow("my_etl") as flow:
        task1.bind(upstream_tasks=[], flow=flow)
        task2.bind(upstream_tasks=[task1], flow=flow)
        task3.bind(upstream_tasks=[task2], flow=flow)
    return flow

