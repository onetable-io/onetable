---
sidebar_position: 2
title: "Docker Playground"
---

# OneTable Playground
This playground helps you install and work with OneTable in a dockerized environment.

## Pre-requisites
* Install Docker in your local machine
* Clone [OneTable GitHub repository](https://github.com/onetable-io/onetable)

:::note NOTE:
This demo was tested in AArch64 based macOS operating system
:::

## Setting up the playground
After cloning the OneTable repository, change directory to `demo` and run the `start_demo.sh` script.
This script builds OneTable jars required for the demo and then spins up docker containers to start a Spark cluster, 
Jupyter notebook with Scala interpreter, Hive Metastore, Presto and Trino.

```shell md title="shell"
cd demo
./start_demo.sh
```

### Accessing the Spark cluster
You can access the Spark master by running `docker exec -it demo-spark-master-1 /bin/bash` in a separate terminal window.
This will open the master node in a bash shell and will initially take you to `/home/onetable` directory.

### Running the tutorial
Once inside the master node, you can follow the [Creating your first interoperable table](/docs/how-to#steps) tutorial.

To run sync, from the same directory, you can create the my_config.yaml file using `vi my_config.yaml` command and copy-paste
the contents from the [run sync](/docs/how-to#running-sync) section and then 
run the `java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar --datasetConfig my_config.yaml` command.
