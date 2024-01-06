# OneTable

[![Build Status](https://dev.azure.com/onetable-io/onetable-io/_apis/build/status%2Fonetable-io.onetable?branchName=main)](https://dev.azure.com/onetable-io/onetable-io/_build/latest?definitionId=1&branchName=main)
[![Site Build Status](https://github.com/onetable-io/onetable/actions/workflows/site-build.yml/badge.svg)](https://github.com/onetable-io/onetable/actions/workflows/site-build.yml)
[![Site Deploy Status](https://github.com/onetable-io/onetable/actions/workflows/site-deploy.yml/badge.svg)](https://github.com/onetable-io/onetable/actions/workflows/site-deploy.yml)
[![Package Deploy Status](https://github.com/onetable-io/onetable/actions/workflows/package-deploy.yml/badge.svg)](https://github.com/onetable-io/onetable/actions/workflows/package-deploy.yml)

OneTable is an omni-directional converter for table formats that facilitates interoperability across data processing systems
and query engines.
Currently, OneTable supports widely adopted open-source table formats such as Apache Hudi, Apache Iceberg, and Delta Lake.

OneTable simplifies data lake operations by leveraging a common model for table representation.
This allows users to write data in one format while still benefiting from integrations and features available in other
formats.
For instance, OneTable enables existing Hudi users to seamlessly work with Databricks's Photon Engine or query Iceberg
tables with Snowflake.
Creating transformations from one format to another is straightforward and only requires the implementation of a few interfaces,
which we believe will facilitate the expansion of supported source and target formats in the future.

[![Twitter Follow](https://img.shields.io/badge/Twitter-1D9BF0.svg?style=for-the-badge&logo=Twitter&logoColor=white)](https://twitter.com/OnetableOSS)
[![Linkedin Follow](https://img.shields.io/badge/LinkedIn-0A66C2.svg?style=for-the-badge&logo=LinkedIn&logoColor=white)](https://www.linkedin.com/company/onetable-oss)
[![READ THE DOCS](https://img.shields.io/badge/Read%20the%20Docs-8CA1AF.svg?style=for-the-badge&logo=Read-the-Docs&logoColor=white)](https://onetable.dev)

# Building the project and running tests.
1. Use Java11 for building the project. If you are using some other java version, you can use [jenv](https://github.com/jenv/jenv) to use multiple java versions locally.
2. Build the project using `mvn clean package`. Use `mvn clean package -DskipTests` to skip tests while building.
3. Use `mvn clean test` or `mvn test` to run all unit tests. If you need to run only a specific test you can do this
   by something like `mvn test -Dtest=TestDeltaSync -pl core`.
4. Similarly, use `mvn clean verify` or `mvn verify` to run integration tests.

# Style guide
1. We use [Maven Spotless plugin](https://github.com/diffplug/spotless/tree/main/plugin-maven) and 
   [Google java format](https://github.com/google/google-java-format) for code style.
2. Use `mvn spotless:check` to find out code style violations and `mvn spotless:apply` to fix them. 
   Code style check is tied to compile phase by default, so code style violations will lead to build failures.

# Running the bundled jar
1. Get a pre-built bundled jar or create the jar with `mvn install -DskipTests`
2. create a yaml file that follows the format below:
```yaml
sourceFormat: HUDI
targetFormats:
  - DELTA
  - ICEBERG
datasets:
  -
    tableBasePath: s3://tpc-ds-datasets/1GB/hudi/call_center
    tableDataPath: s3://tpc-ds-datasets/1GB/hudi/call_center/data
    tableName: call_center
    namespace: my.db
  -
    tableBasePath: s3://tpc-ds-datasets/1GB/hudi/catalog_sales
    tableName: catalog_sales
    partitionSpec: cs_sold_date_sk:VALUE
  -
    tableBasePath: s3://hudi/multi-partition-dataset
    tableName: multi_partition_dataset
    partitionSpec: time_millis:DAY:yyyy-MM-dd,type:VALUE
  -
    tableBasePath: abfs://container@storage.dfs.core.windows.net/multi-partition-dataset
    tableName: multi_partition_dataset
```
- `sourceFormat`  is the format of the source table that you want to convert
- `targetFormats` is a list of formats you want to create from your source tables
- `tableBasePath` is the basePath of the table
- `tableDataPath` is an optional field specifying the path to the data files. If not specified, the tableBasePath will be used. For Iceberg source tables, you will need to specify the `/data` path.
- `namespace` is an optional field specifying the namespace of the table and will be used when syncing to a catalog.
- `partitionSpec` is a spec that allows us to infer partition values. This is only required for Hudi source tables. If the table is not partitioned, leave it blank. If it is partitioned, you can specify a spec with a comma separated list with format `path:type:format`
  - `path` is a dot separated path to the partition field
  - `type` describes how the partition value was generated from the column value
    - `VALUE`: an identity transform of field value to partition value
    - `YEAR`: data is partitioned by a field representing a date and year granularity is used
    - `MONTH`: same as `YEAR` but with month granularity
    - `DAY`: same as `YEAR` but with day granularity
    - `HOUR`: same as `YEAR` but with hour granularity
  - `format`: if your partition type is `YEAR`, `MONTH`, `DAY`, or `HOUR` specify the format for the date string as it appears in your file paths
3. The default implementations of table format clients can be replaced with custom implementations by specifying a client configs yaml file in the format below:
```yaml
# sourceClientProviderClass: The class name of a table format's client factory, where the client is
#     used for reading from a table of this format. All user configurations, including hadoop config
#     and client specific configuration, will be available to the factory for instantiation of the
#     client.
# targetClientProviderClass: The class name of a table format's client factory, where the client is
#     used for writing to a table of this format.
# configuration: A map of configuration values specific to this client.
tableFormatsClients:
    HUDI:
      sourceClientProviderClass: io.onetable.hudi.HudiSourceClientProvider
    DELTA:
      targetClientProviderClass: io.onetable.delta.DeltaClient
      configuration:
        spark.master: local[2]
        spark.app.name: onetableclient
```
4. A catalog can be used when reading and updating Iceberg tables. The catalog can be specified in a yaml file and passed in with the `--icebergCatalogConfig` option. The format of the catalog config file is:
```yaml
catalogImpl: io.my.CatalogImpl
catalogName: name
catalogOptions: # all other options are passed through in a map
  key1: value1
  key2: value2
```
5. run with `java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar --datasetConfig my_config.yaml [--hadoopConfig hdfs-site.xml] [--clientsConfig clients.yaml] [--icebergCatalogConfig catalog.yaml]`
The bundled jar includes hadoop dependencies for AWS, Azure, and GCP. Authentication for AWS is done with 
`com.amazonaws.auth.DefaultAWSCredentialsProviderChain`. To override this setting, specify a different implementation 
with the `--awsCredentialsProvider` option.

# Contributing
## Setup
For setting up the repo on IntelliJ, open the project and change the java version to Java11 in File->ProjectStructure
![img.png](style/IDE.png)

You have found a bug, or have a cool idea you that want to contribute to the project ? Please file a GitHub issue [here](https://github.com/onetable-io/onetable/issues)

## Adding a new target format
Adding a new target format requires a developer implement [TargetClient](./api/src/main/java/io/onetable/spi/sync/TargetClient.java). Once you have implemented that interface, you can integrate it into the [OneTableClient](./core/src/main/java/io/onetable/client/OneTableClient.java). If you think others may find that target useful, please raise a Pull Request to add it to the project.

## Overview of the sync process
![img.png](assets/images/sync_flow.jpg)
