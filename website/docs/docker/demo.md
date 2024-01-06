---
sidebar_position: 1
title: "Docker Demo"
---

# Building interoperable tables using OneTable 
This demo walks you through a fictional use case and the steps to add interoperability between table formats using OneTable.
For this purpose, a self-contained data infrastructure is brought up as Docker containers within your computer.


## Pre-requisites
* Install Docker in your local machine
* Clone [OneTable GitHub repository](https://github.com/onetable-io/onetable)

:::note NOTE:
This demo was tested in both x86-64 and AArch64 based macOS operating systems
:::

## Setting up Docker cluster
After cloning the OneTable repository, change directory to `demo` and run the `start_demo.sh` script.
This script builds OneTable jars required for the demo and then spins up docker containers to start a Jupyter notebook
with Scala interpreter, Hive Metastore, Presto and Trino.

```shell md title="shell"
cd demo
./start_demo.sh
```

### Accessing Services
#### Trino
You can access the local Trino container by running `docker exec -it trino trino`

#### Presto
You can access the local Presto container by running `docker exec -it presto presto-cli --server localhost:8082`

#### Jupyter Notebook
To access the notebook, look for a log line during startup that contains `To access the server, open this file in a 
browser: ...  Or copy and paste one of these URLs: ...` and use the link that starts with http://127.0.0.1:8888/
to open the notebook in your browser. 

The demo is located at `work/demo.ipynb`. The notebook also includes helpful markdowns explaining the steps.
