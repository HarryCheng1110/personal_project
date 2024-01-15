# Overview
It is a repository where I put my projects and examples of packages that I find useful for my work

## dask_example/sql_with_dask_example.py
dask is a Python package that can handle huge dataset with parallel computing. 

It has many functionalities, but I mainly use it when querying a large dataset, where I know it needs to be split into several small batches to avoid running the risk of breaking my PC or the server. <br />

Below is the screenshot of the result of the py file, showing how much time it saves with dask comparing to for loop. <br />

![image](https://github.com/HarryCheng1110/personal_project/assets/29909951/40420b36-64be-4ec0-8c1e-8b81c26d0477)

## airflow_example/dag_example.py
I recently wrote a code where it gets and transform data from yesterday and saves the csv result in a folder. It is scheduled to run every morning, so I decided to learn to use Airflow for better and easier management (I used to use crontab or windows scheduler)

This example contains very roughly how my actual code in work processes since I can't really show it. And below is the graph on Airflow showing the dependencies.

![image](screenshot\airflow_dag.png)