# -*- coding: utf-8 -*-
"""Fields here are taken from the Databricks API docs.

Most are left the same, but some have been modified to better express
the requirements.

See:
- https://docs.databricks.com/dev-tools/api/latest/jobs.html
- https://docs.databricks.com/dev-tools/api/latest/clusters.html
- https://docs.databricks.com/dev-tools/api/latest/libraries.html
"""
from typing import Dict, Union

from dagster import Array, Bool, Enum, EnumValue, Field, Int, Permissive, Selector, Shape, String


def _define_autoscale() -> Field:
    return Field(
        Shape(
            fields={
                "min_workers": Field(
                    Int,
                    description=(
                        "The minimum number of workers to which the cluster can scale down "
                        "when underutilized. It is also the initial number of workers the cluster "
                        "will have after creation."
                    ),
                ),
                "max_workers": Field(
                    Int,
                    description=(
                        "The maximum number of workers to which the cluster can scale up "
                        "when overloaded. max_workers must be strictly greater than min_workers."
                    ),
                ),
            }
        )
    )


def _define_size() -> Selector:
    num_workers = Field(
        Int,
        description=(
            "If num_workers, number of worker nodes that this cluster should have. "
            "A cluster has one Spark Driver and num_workers Executors for a total of "
            "num_workers + 1 Spark nodes."
        ),
        is_required=True,
    )
    return Selector({"autoscale": _define_autoscale(), "num_workers": num_workers})


def _define_custom_tags() -> Field:
    key = Field(
        String,
        description=(
            "The key of the tag. The key length must be between 1 and 127 UTF-8 "
            "characters, inclusive. For a list of all restrictions, see AWS Tag Restrictions: "
            "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions"
        ),
        is_required=True,
    )
    value = Field(
        String,
        description=(
            "The value of the tag. The value length must be less than or equal to "
            "255 UTF-8 characters. For a list of all restrictions, see AWS Tag Restrictions: "
            "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions"
        ),
        is_required=True,
    )
    return Field(
        [Shape(fields={"key": key, "value": value})],
        description=(
            "Additional tags for cluster resources. Databricks tags all cluster resources (e.g.,"
            " AWS instances and EBS volumes) with these tags in addition to default_tags. Note: -"
            " Tags are not supported on legacy node types such as compute-optimized and"
            " memory-optimized - Databricks allows at most 45 custom tagsMore restrictions may"
            " apply if using Azure Databricks; refer to the official docs for further details."
        ),
        is_required=False,
    )


def _define_dbfs_storage_info() -> Field:
    destination = Field(String, description="DBFS destination, e.g. dbfs:/my/path")
    return Field(Shape(fields={"destination": destination}), description="DBFS storage information")


def _define_s3_storage_info() -> Field:
    destination = Field(
        String,
        description=(
            "S3 destination, e.g. s3://my-bucket/some-prefix. "
            "You must configure the cluster with an instance profile and the instance profile "
            "must have write access to the destination. You cannot use AWS keys."
        ),
    )
    region = Field(
        String,
        description=(
            "S3 region, e.g. us-west-2. Either region or endpoint must be set. "
            "If both are set, endpoint is used."
        ),
    )
    endpoint = Field(
        String,
        description=(
            "S3 endpoint, e.g. https://s3-us-west-2.amazonaws.com. "
            "Either region or endpoint must be set. If both are set, endpoint is used."
        ),
    )
    enable_encryption = Field(
        Bool,
        description="(Optional) Enable server side encryption, false by default.",
        is_required=False,
    )
    encryption_type = Field(
        String,
        description=(
            "(Optional) The encryption type, it could be sse-s3 or sse-kms. "
            "It is used only when encryption is enabled and the default type is sse-s3."
        ),
        is_required=False,
    )
    kms_key = Field(
        String,
        description=(
            "(Optional) KMS key used if encryption is enabled and encryption type is set "
            "to sse-kms."
        ),
        is_required=False,
    )
    canned_acl = Field(
        String,
        description=(
            "(Optional) Set canned access control list, e.g. bucket-owner-full-control.If"
            " canned_acl is set, the cluster instance profile must have s3:PutObjectAcl permission"
            " on the destination bucket and prefix. The full list of possible canned ACLs can be"
            " found at"
            " https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl. By"
            " default only the object owner gets full control. If you are using cross account role"
            " for writing data, you may want to set bucket-owner-full-control to make bucket owner"
            " able to read the logs."
        ),
        is_required=False,
    )
    return Field(
        Shape(
            fields={
                "destination": destination,
                "region": region,
                "endpoint": endpoint,
                "enable_encryption": enable_encryption,
                "encryption_type": encryption_type,
                "kms_key": kms_key,
                "canned_acl": canned_acl,
            }
        ),
        description="S3 storage information",
    )


def _define_aws_attributes_conf() -> Field:
    return Field(
        Permissive(
            fields={
                "first_on_demand": Field(
                    Int,
                    description=(
                        "The first first_on_demand nodes of the cluster will be placed on on-demand"
                        " instances. If this value is greater than 0, the cluster driver node will"
                        " be placed on an on-demand instance. If this value is greater than or"
                        " equal to the current cluster size, all nodes will be placed on on-demand"
                        " instances. If this value is less than the current cluster size,"
                        " first_on_demand nodes will be placed on on-demand instances and the"
                        " remainder will be placed on availability instances. This value does not"
                        " affect cluster size and cannot be mutated over the lifetime of a cluster."
                    ),
                    is_required=False,
                ),
                "availability": Field(
                    Enum(
                        "AWSAvailability",
                        [
                            EnumValue("SPOT"),
                            EnumValue("ON_DEMAND"),
                            EnumValue("SPOT_WITH_FALLBACK"),
                        ],
                    ),
                    description=(
                        "Availability type used for all subsequent nodes past the first_on_demand"
                        " ones. Note: If first_on_demand is zero, this availability type will be"
                        " used for the entire cluster."
                    ),
                    is_required=False,
                ),
                "zone_id": Field(
                    String,
                    description=(
                        "Identifier for the availability zone/datacenter in which the cluster"
                        " resides."
                    ),
                    is_required=False,
                ),
                "instance_profile_arn": Field(
                    String,
                    description=(
                        "Nodes for this cluster will only be placed on AWS instances with this"
                        " instance profile."
                    ),
                    is_required=False,
                ),
                "spot_bid_price_percent": Field(
                    Int,
                    description=(
                        "The max price for AWS spot instances, as a percentage of the corresponding"
                        " instance type's on-demand price."
                    ),
                    is_required=False,
                ),
                "ebs_volume_type": Field(
                    Enum(
                        "EBSVolumeType",
                        [EnumValue("GENERAL_PURPOSE_SSD"), EnumValue("THROUGHPUT_OPTIMIZED_HDD")],
                    ),
                    description="The type of EBS volumes that will be launched with this cluster.",
                    is_required=False,
                ),
                "ebs_volume_count": Field(
                    Int,
                    description=(
                        "The number of volumes launched for each instance. You can choose up to 10"
                        " volumes."
                    ),
                    is_required=False,
                ),
                "ebs_volume_size": Field(
                    Int,
                    description="The size of each EBS volume (in GiB) launched for each instance.",
                    is_required=False,
                ),
                "ebs_volume_iops": Field(
                    Int, description="The number of IOPS per EBS gp3 volume.", is_required=False
                ),
                "ebs_volume_throughput": Field(
                    Int,
                    description="The throughput per EBS gp3 volume, in MiB per second.",
                    is_required=False,
                ),
            }
        ),
        description=(
            "Attributes related to clusters running on Amazon Web Services. "
            "If not specified at cluster creation, a set of default values is used. "
            "See aws_attributes at https://docs.databricks.com/dev-tools/api/latest/clusters.html."
        ),
        is_required=False,
    )


def _define_cluster_log_conf() -> Field:
    return Field(
        Selector({"dbfs": _define_dbfs_storage_info(), "s3": _define_s3_storage_info()}),
        description=(
            "Recommended! The configuration for delivering Spark logs to a long-term storage"
            " destination. Only one destination can be specified for one cluster. If the conf is"
            " given, the logs will be delivered to the destination every 5 mins. The destination of"
            " driver logs is <destination>/<cluster-id>/driver, while the destination of executor"
            " logs is <destination>/<cluster-id>/executor."
        ),
        is_required=False,
    )


def _define_init_script():
    return Selector({"dbfs": _define_dbfs_storage_info(), "s3": _define_s3_storage_info()})


def _define_node_types() -> Field:
    node_type_id = Field(
        String,
        description=(
            "This field encodes, through a single value, the resources available to each "
            "of the Spark nodes in this cluster. For example, the Spark nodes can be provisioned "
            "and optimized for memory or compute intensive workloads. "
            "A list of available node types can be retrieved by using the List node types API "
            "call. This field is required."
        ),
        is_required=True,
    )

    driver_node_type_id = Field(
        String,
        description=(
            "The node type of the Spark driver. "
            "This field is optional; if unset, the driver node type is set as the "
            "same value as node_type_id defined above."
        ),
        is_required=False,
    )

    return Field(
        Shape(fields={"node_type_id": node_type_id, "driver_node_type_id": driver_node_type_id})
    )


def _define_nodes() -> Field:
    instance_pool_id = Field(
        String,
        description=(
            "The optional ID of the instance pool to which the cluster belongs. "
            "Refer to the Instance Pools API for details."
        ),
        is_required=False,
    )

    return Field(
        Selector({"node_types": _define_node_types(), "instance_pool_id": instance_pool_id}),
        description=(
            "The nodes used in the cluster. Either the node types or an instance pool "
            "can be specified."
        ),
        is_required=True,
    )


def _define_new_cluster() -> Field:
    spark_version = Field(
        String,
        description=(
            "The Spark version of the cluster. "
            "A list of available Spark versions can be retrieved by using the "
            "Runtime versions API call. This field is required."
        ),
        is_required=True,
    )

    spark_conf = Field(
        Permissive(),
        description=(
            "An object containing a set of optional, user-specified Spark configuration key-value"
            " pairs. You can also pass in a string of extra JVM options to the driver and the"
            " executors via spark.driver.extraJavaOptions and spark.executor.extraJavaOptions"
            ' respectively. Example Spark confs: {"spark.speculation": true,'
            ' "spark.streaming.ui.retainedBatches": 5} or {"spark.driver.extraJavaOptions":'
            ' "-verbose:gc -XX:+PrintGCDetails"}'
        ),
        is_required=False,
    )

    ssh_public_keys = Field(
        [String],
        description=(
            "SSH public key contents that will be added to each Spark node in this cluster. The"
            " corresponding private keys can be used to login with the user name ubuntu on port"
            " 2200. Up to 10 keys can be specified."
        ),
        is_required=False,
    )

    init_scripts = Field(
        [_define_init_script()],
        description=(
            "The configuration for storing init scripts. Any number of scripts can be "
            "specified. The scripts are executed sequentially in the order provided. "
            "If cluster_log_conf is specified, init script logs are sent to "
            "<destination>/<cluster-id>/init_scripts."
        ),
        is_required=False,
    )

    spark_env_vars = Field(
        Permissive(),
        description=(
            "An object containing a set of optional, user-specified environment variable key-value"
            ' pairs. Key-value pair of the form (X,Y) are exported as is (i.e., export X="Y") while'
            " launching the driver and workers. To specify an additional set of"
            " SPARK_DAEMON_JAVA_OPTS, we recommend appending them to $SPARK_DAEMON_JAVA_OPTS as"
            " shown in the example below. This ensures that all default Databricks managed"
            " environmental variables are included as well. Example Spark environment variables:"
            ' {"SPARK_WORKER_MEMORY": "28000m", "SPARK_LOCAL_DIRS": "/local_disk0"} or'
            ' {"SPARK_DAEMON_JAVA_OPTS": "$SPARK_DAEMON_JAVA_OPTS'
            ' -Dspark.shuffle.service.enabled=true"}'
        ),
        is_required=False,
    )

    enable_elastic_disk = Field(
        Bool,
        description=(
            "Autoscaling Local Storage: when enabled, this cluster dynamically acquires attitional"
            " disk space when its Spark workers are running low on disk space. This feature"
            " requires specific AWS permissions to function correctly - refer to"
            " https://docs.databricks.com/clusters/configure.html#autoscaling-local-storage for"
            " details."
        ),
        is_required=False,
    )

    return Field(
        Shape(
            fields={
                "size": _define_size(),
                "spark_version": spark_version,
                "spark_conf": spark_conf,
                "nodes": _define_nodes(),
                "aws_attributes": _define_aws_attributes_conf(),
                "ssh_public_keys": ssh_public_keys,
                "custom_tags": _define_custom_tags(),
                "cluster_log_conf": _define_cluster_log_conf(),
                "init_scripts": init_scripts,
                "spark_env_vars": spark_env_vars,
                "enable_elastic_disk": enable_elastic_disk,
            }
        )
    )


def _define_cluster() -> Selector:
    existing_cluster_id = Field(
        String,
        description=(
            "The ID of an existing cluster that will be used for all runs "
            "of this job. When running jobs on an existing cluster, you may "
            "need to manually restart the cluster if it stops responding. "
            "Databricks suggests running jobs on new clusters for "
            "greater reliability."
        ),
        is_required=True,
    )
    return Selector({"new": _define_new_cluster(), "existing": existing_cluster_id})


def _define_pypi_library() -> Field:
    package = Field(
        String,
        description=(
            "The name of the PyPI package to install. "
            "An optional exact version specification is also supported. "
            "Examples: "
            "- simplejson "
            "- simplejson==3.8.0"
        ),
        is_required=True,
    )
    repo = Field(
        String,
        description=(
            "The repository where the package can be found. "
            "If not specified, the default pip index is used."
        ),
        is_required=False,
    )
    return Field(
        Shape(fields={"package": package, "repo": repo}),
        description="Specification of a PyPI library to be installed.",
    )


def _define_maven_library() -> Field:
    coordinates = Field(
        String,
        description=(
            "Gradle-style Maven coordinates. For example: org.jsoup:jsoup:1.7.2. "
            "This field is required."
        ),
        is_required=True,
    )
    repo = Field(
        String,
        description=(
            "Maven repo to install the Maven package from. "
            "If omitted, both Maven Central Repository and Spark Packages are searched."
        ),
        is_required=False,
    )
    exclusions = Field(
        [String],
        description=(
            "List of dependences to exclude. For example: "
            '["slf4j:slf4j", "*:hadoop-client"]. '
            "Maven dependency exclusions: "
            "https://maven.apache.org/guides/introduction/introduction-to-optional-and-excludes-dependencies.html."
        ),
        is_required=False,
    )
    return Field(
        Shape(fields={"coordinates": coordinates, "repo": repo, "exclusions": exclusions}),
        description="Specification of a Maven library to be installed.",
    )


def _define_cran_library() -> Field:
    package = Field(
        String,
        description="The name of the CRAN package to install. This field is required.",
        is_required=True,
    )
    repo = Field(
        String,
        description=(
            "The repository where the package can be found. "
            "If not specified, the default CRAN repo is used."
        ),
        is_required=False,
    )
    return Field(
        Shape(fields={"package": package, "repo": repo}),
        description="Specification of a CRAN library to be installed.",
    )


def _define_libraries() -> Field:
    jar = Field(
        String,
        description=(
            "URI of the JAR to be installed. DBFS and S3 URIs are supported. "
            'For example: { "jar": "dbfs:/mnt/databricks/library.jar" } or '
            '{ "jar": "s3://my-bucket/library.jar" }. If S3 is used, make sure the cluster has '
            "read access on the library. "
            "You may need to launch the cluster with an instance profile to access the S3 URI."
        ),
    )
    egg = Field(
        String,
        description=(
            "URI of the egg to be installed. DBFS and S3 URIs are supported. "
            'For example: { "egg": "dbfs:/my/egg" } or { "egg": "s3://my-bucket/egg" }. '
            "If S3 is used, make sure the cluster has read access on the library. "
            "You may need to launch the cluster with an instance profile to access the S3 URI. "
        ),
    )
    whl = Field(
        String,
        description=(
            "URI of the wheel or zipped wheels to be installed. DBFS and S3 URIs are "
            'supported. For example: { "whl": "dbfs:/my/whl" } or { "whl": "s3://my-bucket/whl" }.'
            "If S3 is used, make sure the cluster has read access on the library. "
            "You may need to launch the cluster with an instance profile to access the S3 URI. "
            "Also the wheel file name needs to use the correct convention "
            "(https://www.python.org/dev/peps/pep-0427/#file-format). "
            "If zipped wheels are to be installed, the file name suffix should be .wheelhouse.zip."
        ),
    )
    return Field(
        [
            Selector(
                {
                    "jar": jar,
                    "egg": egg,
                    "whl": whl,
                    "pypi": _define_pypi_library(),
                    "maven": _define_maven_library(),
                    "cran": _define_cran_library(),
                }
            )
        ],
        description=(
            "An optional list of libraries to be installed on the cluster that will "
            "execute the job. By default dagster, dagster-databricks and dagster-pyspark libraries "
            "will be included."
        ),
        is_required=False,
    )


def _define_submit_run_fields() -> Dict[str, Union[Selector, Field]]:
    run_name = Field(
        String,
        description="An optional name for the run. The default value is Untitled",
        is_required=False,
    )
    timeout_seconds = Field(
        Int,
        description=(
            "An optional timeout applied to each run of this job. "
            "The default behavior is to have no timeout."
        ),
        is_required=False,
    )
    idempotency_token = Field(
        String,
        description=(
            "An optional token that can be used to guarantee the idempotency of job run requests."
            "If an active run with the provided token already exists, the request will not create "
            "a new run, but will return the ID of the existing run instead. "
            "If you specify the idempotency token, upon failure you can retry until the request "
            "succeeds. Databricks guarantees that exactly one run will be launched with that "
            "idempotency token. "
            "This token should have at most 64 characters."
        ),
        is_required=False,
    )
    install_default_libraries = Field(
        Bool,
        description=(
            "By default, Dagster installs a version of dagster, dagster-databricks, and"
            " dagster-pyspark matching the locally-installed versions of those libraries. If you"
            " would like to disable this behavior, this value can be set to False."
        ),
        is_required=False,
    )
    return {
        "cluster": _define_cluster(),
        "run_name": run_name,
        "libraries": _define_libraries(),
        "install_default_libraries": install_default_libraries,
        "timeout_seconds": timeout_seconds,
        "idempotency_token": idempotency_token,
    }


def _define_notebook_task() -> Field:
    notebook_path = Field(
        String,
        description=(
            "The absolute path of the notebook to be run in the Databricks Workspace. "
            "This path must begin with a slash. This field is required."
        ),
        is_required=True,
    )
    base_parameters = Field(
        Permissive(),
        description=(
            "Base parameters to be used for each run of this job. "
            "If the notebook takes a parameter that is not specified in the job's base_parameters "
            "or the run-now override parameters, the default value from the notebook will be used. "
            "Retrieve these parameters in a notebook by using dbutils.widgets.get()."
        ),
        is_required=False,
    )
    return Field(Shape(fields={"notebook_path": notebook_path, "base_parameters": base_parameters}))


def _define_spark_jar_task() -> Field:
    main_class_name = Field(
        String,
        description=(
            "The full name of the class containing the main method to be executed. "
            "This class must be contained in a JAR provided as a library. "
            "The code should use SparkContext.getOrCreate to obtain a Spark context; "
            "otherwise, runs of the job will fail."
        ),
        is_required=True,
    )
    parameters = Field(
        [String],
        description="Parameters that will be passed to the main method.",
        is_required=False,
        default_value=[],
    )
    return Field(Shape(fields={"main_class_name": main_class_name, "parameters": parameters}))


def _define_spark_python_task() -> Field:
    python_file = Field(
        String,
        description=(
            "The URI of the Python file to be executed. DBFS and S3 paths are supported. If a"
            " `git_source` is set, then the path is relative to the git source root.This field is"
            " required."
        ),
        is_required=True,
    )
    parameters = Field(
        [String],
        description="Command line parameters that will be passed to the Python file.",
        is_required=False,
        default_value=[],
    )
    git_source = Field(
        Shape(
            fields={
                "git_url": Field(String, description="The URL of the git repository."),
                "git_branch": Field(String, description="The git branch to use."),
            },
        ),
        description="The git repository/branch with which to resolve the `python_file` path.",
    )
    return Field(
        Shape(
            fields={"python_file": python_file, "parameters": parameters, "git_source": git_source}
        )
    )


def _define_spark_submit_task() -> Field:
    parameters = Field(
        [String],
        description="Command-line parameters passed to spark submit.",
        is_required=True,
    )
    return Field(
        Shape(fields={"parameters": parameters}),
        description=(
            "Important!You can Spark submit tasks only on new clusters. In the new_cluster"
            " specification, libraries and spark_conf are not supported. Instead, use --jars and"
            " --py-files to add Java and Python libraries and use --conf to set the Spark"
            " configuration. master, deploy-mode, and executor-cores are automatically configured"
            " by Databricks; you cannot specify them in parameters. By default, the Spark submit"
            " job uses all available memory (excluding reserved memory for Databricks services)."
            " You can set --driver-memory, and --executor-memory to a smaller value to leave some"
            " room for off-heap usage. The --jars, --py-files, --files arguments support DBFS and"
            " S3 paths."
        ),
    )


def _define_task() -> Field:
    return Field(
        Selector(
            {
                "notebook_task": _define_notebook_task(),
                "spark_jar_task": _define_spark_jar_task(),
                "spark_python_task": _define_spark_python_task(),
                "spark_submit_task": _define_spark_submit_task(),
            }
        ),
        description="The task to run.",
        is_required=True,
    )


def define_databricks_submit_custom_run_config() -> Field:
    fields = _define_submit_run_fields()
    fields["task"] = _define_task()
    return Field(Shape(fields=fields), description="Databricks job run configuration")


def define_databricks_submit_run_config() -> Field:
    return Field(
        Shape(fields=_define_submit_run_fields()),
        description="Databricks job run configuration",
    )


def _define_secret_scope() -> Field:
    return Field(
        String,
        description="The Databricks secret scope containing the storage secrets.",
        is_required=True,
    )


def _define_s3_storage_credentials() -> Field:
    access_key_key = Field(
        String,
        description="The key of a Databricks secret containing the S3 access key ID.",
        is_required=True,
    )
    secret_key_key = Field(
        String,
        description="The key of a Databricks secret containing the S3 secret access key.",
        is_required=True,
    )
    return Field(
        Shape(
            fields={
                "secret_scope": _define_secret_scope(),
                "access_key_key": access_key_key,
                "secret_key_key": secret_key_key,
            }
        ),
        description="S3 storage secret configuration",
    )


def _define_adls2_storage_credentials() -> Field:
    storage_account_name = Field(
        String,
        description="The name of the storage account used to access data.",
        is_required=True,
    )
    storage_account_key_key = Field(
        String,
        description="The key of a Databricks secret containing the storage account secret key.",
        is_required=True,
    )
    return Field(
        Shape(
            fields={
                "secret_scope": _define_secret_scope(),
                "storage_account_name": storage_account_name,
                "storage_account_key_key": storage_account_key_key,
            }
        ),
        description="ADLS2 storage secret configuration",
    )


def define_databricks_storage_config() -> Field:
    return Field(
        Selector(
            {
                "s3": _define_s3_storage_credentials(),
                "adls2": _define_adls2_storage_credentials(),
            }
        ),
        description=(
            "Databricks storage configuration for either S3 or ADLS2. If access credentials for"
            " your Databricks storage are stored in Databricks secrets, this config indicates the"
            " secret scope and the secret keys used to access either S3 or ADLS2."
        ),
        is_required=False,
    )


def define_databricks_env_variables() -> Field:
    return Field(
        Permissive(),
        description=(
            "Dictionary of arbitrary environment variables to be set on the databricks cluster."
        ),
        is_required=False,
    )


def define_databricks_secrets_config() -> Field:
    name = Field(
        String,
        description="The environment variable name, e.g. `DATABRICKS_TOKEN`.",
        is_required=True,
    )
    key = Field(String, description="The key of the Databricks secret.", is_required=True)
    scope = Field(String, description="The scope of the Databricks secret.", is_required=True)
    return Field(
        [Shape(fields={"name": name, "key": key, "scope": scope})],
        description=(
            "Databricks secrets to be exported as environment variables. Since runs will execute in"
            " the Databricks runtime environment, environment variables (such as those required for"
            " a `StringSource` config variable) will not be accessible to Dagster. These variables"
            " must be stored as Databricks secrets and specified here, which will ensure they are"
            " re-exported as environment variables accessible to Dagster upon execution."
        ),
        is_required=False,
    )


def _define_accessor() -> Selector:
    return Selector(
        {"group_name": str, "user_name": str},
        description="Group or User that shall access the target.",
    )


def _define_databricks_job_permission() -> Field:
    job_permission_levels = [
        "NO_PERMISSIONS",
        "CAN_VIEW",
        "CAN_MANAGE_RUN",
        "IS_OWNER",
        "CAN_MANAGE",
    ]
    return Field(
        {
            permission_level: Field(Array(_define_accessor()), is_required=False)
            for permission_level in job_permission_levels
        },
        description=(
            "job permission spec; ref:"
            " https://docs.databricks.com/security/access-control/jobs-acl.html#job-permissions"
        ),
        is_required=False,
    )


def _define_databricks_cluster_permission() -> Field:
    cluster_permission_levels = ["NO_PERMISSIONS", "CAN_ATTACH_TO", "CAN_RESTART", "CAN_MANAGE"]
    return Field(
        {
            permission_level: Field(Array(_define_accessor()), is_required=False)
            for permission_level in cluster_permission_levels
        },
        description=(
            "cluster permission spec; ref:"
            " https://docs.databricks.com/security/access-control/cluster-acl.html#cluster-level-permissions"
        ),
        is_required=False,
    )


def define_databricks_permissions() -> Field:
    return Field(
        {
            "job_permissions": _define_databricks_job_permission(),
            "cluster_permissions": _define_databricks_cluster_permission(),
        }
    )
