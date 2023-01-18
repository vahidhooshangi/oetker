#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the @taskgroup decorator."""
from __future__ import annotations

import logging
from enum import Enum
from typing import Dict

import pendulum
import requests

from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_env = {
    'file_path': ''
}

env = Variable.get("oetker", default_var=default_env, deserialize_json=True)


class Config(Enum):
    file_path = env['file_path']

    @classmethod
    def to_params(cls) -> Dict[str, str]:
        out = {}
        for k, v in cls.__members__.items():
            out[k] = v.value

        return out


@task
def task_start():
    """Empty Task which is First Task of Dag"""
    return "[Task_start]"


@task
def download_data_set():
    res = requests.get("https://storage.googleapis.com/datascience-public/data-eng-challenge/MOCK_DATA.json")
    with open("/home/airflow/response.json", "w") as f:
        logging.info(res.text)
        f.write(res.text)


submit_job = SparkSubmitOperator(
    application="spark_batch.py",
    task_id="submit_pyspark_job",
    params=Config.to_params()
)


@task
def task_1(value: int) -> str:
    """Empty Task1"""
    return f"[ Task1 {value} ]"


@task
def task_2(value: str) -> str:
    """Empty Task2"""
    return f"[ Task2 {value} ]"


@task
def task_3(value: str) -> None:
    """Empty Task3"""
    print(f"[ Task3 {value} ]")


@task
def task_end():
    """Empty Task which is Last Task of Dag"""
    print("[ Task_End  ]")


with DAG(
        dag_id="oetker",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["Interview"],
) as dag:
    start_task = task_start()
    end_task = task_end()
    download_data_set_task = download_data_set()

    start_task >> download_data_set_task >> submit_job >> end_task

