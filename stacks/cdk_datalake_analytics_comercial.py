from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    CfnOutput,
    aws_lambda_event_sources as lambda_event_sources,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    aws_s3_notifications as s3n,
    aws_apigateway as apigw,
    aws_glue_alpha as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks
)
from constructs import Construct
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.models.configs import *
from aje_cdk_libs.constants.environments import Environments
from constants.paths import Paths
from constants.layers import Layers
import os
from dotenv import load_dotenv
import urllib.parse
from aje_cdk_libs.constants.project_config import ProjectConfig

class GlueJobAnalyticsConfig:
    layer: str
    job_name: str
    periods: list
    num_workers: int
    script_path: str
    environment: dict
    role: iam.IRole

class CdkDatalakeAnaliticsComercialStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, project_config: ProjectConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)         
        self.PROJECT_CONFIG = project_config        
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        self.Paths = Paths(self.PROJECT_CONFIG.app_config)
        self.Layers = Layers(self.PROJECT_CONFIG.app_config, project_config.region_name, project_config.account_id)
        self.TEAM = self.PROJECT_CONFIG.app_config["team"]
        self.BUSINESS_PROCESS = self.PROJECT_CONFIG.app_config["business_process"]
        self.DOMAIN = self.PROJECT_CONFIG.app_config["domain"]
        self.ANALYTICS = self.PROJECT_CONFIG.app_config["analytics"]
        
        self.import_s3_buckets()
        self.import_dynamodb_tables()
        self.import_sns_topics()
        self.deployment_s3_buckets()
        self.create_lambda_layers()
        self.create_lambdas()
        self.create_glue_jobs()
        self.create_step_functions()
        
    def import_s3_buckets(self):
        """Import an existing S3 bucket"""
        self.s3_artifacts_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["artifacts"])
        self.s3_athena_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["athena"])
        self.s3_external_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["external"])
        self.s3_landing_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["landing"])
        self.s3_raw_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["raw"])
        self.s3_stage_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["stage"])
        self.s3_analytics_bucket = self.builder.import_s3_bucket(self.PROJECT_CONFIG.app_config["s3_buckets"]["analytics"])
    
    def import_dynamodb_tables(self):
        """Import an existing DynamoDB table"""
        self.dynamodb_configuration_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["configuration"])
        self.dynamodb_credentials_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["credentials"])
        self.dynamodb_columns_specifications_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["columns-specifications"])
        self.dynamodb_logs_table = self.builder.import_dynamodb_table(self.PROJECT_CONFIG.app_config["dynamodb_tables"]["logs"])
        
    def deployment_s3_buckets(self):
        """Import an existing S3 bucket"""
        
        resource_name = "layer_analytics"
        config = S3DeploymentConfig(
            f"BucketDeploymentJobsGlue{resource_name}",
            [s3_deployment.Source.asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_LAYER}")],
            self.s3_artifacts_bucket,
            f"{self.Paths.AWS_ARTIFACTS_GLUE_LAYER}"
        )
        
        self.builder.deploy_s3_bucket(config)
        
        resource_name = "csv_analytics"
        config = S3DeploymentConfig(
            f"BucketDeploymentJobsGlue{resource_name}",
            [s3_deployment.Source.asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CSV}")],
            self.s3_artifacts_bucket,
            f"{self.Paths.AWS_ARTIFACTS_GLUE_CSV}"
        )
        
        self.builder.deploy_s3_bucket(config)
        
        resource_name = "code_analytics"
        config = S3DeploymentConfig(
            f"BucketDeploymentJobsGlue{resource_name}",
            [s3_deployment.Source.asset(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CODE}")],
            self.s3_artifacts_bucket,
            f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}"
        )
        
        self.builder.deploy_s3_bucket(config)
    
    def __load_data_config(file_path: str):
        """Load the Glue job configuration from a JSON file"""
        data = []
        df = pd.read_csv(file_path, delimiter = ';')
        for index, row in df.iterrows():
            data.append(row.to_dict())
        return data

    def __build_glue_job(self, layer: str):
        """Build a Glue job"""

        jobs_args = {
            '--extra-py-files' : f's3://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE_LAYER}/common_jobs_functions.py',
            '--S3_PATH_STG': f"s3a://{self.s3_stage_bucket.bucket_name}/{self.TEAM}/",
            '--S3_PATH_ANALYTICS' : f"s3a://{self.s3_analytics_bucket.bucket_name}/{self.TEAM}/{self.BUSINESS_PROCESS}/",
            '--S3_PATH_EXTERNAL' : f"s3a://{self.s3_external_bucket.bucket_name}/{self.TEAM}/",
            '--S3_PATH_ARTIFACTS' : f"s3a://{self.s3_landing_bucket.bucket_name}/{self.TEAM}/{self.BUSINESS_PROCESS}/",
            '--S3_PATH_ARTIFACTS_CSV': f"s3a://{self.s3_artifacts_bucket.bucket_name}/{self.Path.AWS_ARTIFACTS_GLUE_CSV}",
            '--CATALOG_CONNECTION' : None,
            '--REGION_NAME' : self.PROJECT_CONFIG.region_name,
            '--DYNAMODB_DATABASE_NAME' : self.dynamodb_credentials_table.table_name,
            '--DYNAMODB_LOGS_TABLE' : self.dynamodb_logs_table.table_name,
            '--COD_PAIS' : 'PE',
            '--INSTANCIAS': 'PE',
            '--COD_SUBPROCESO': '0',
            '--ERROR_TOPIC_ARN' : self.sns_failed_topic.topic_arn,
            '--PROJECT_NAME' : self.PROJECT_CONFIG.app_config["team"],
            '--datalake-formats' : "delta",
            '--enable-continuous-log-filter' : "true"
        }

        file_path = f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CONFIG}/{layer}"
        
        configs = self.__load_data_config(file_path)
        for config in configs:
            job_name=f"{config["layer"]}-{config["job_name"]}"
            config["environment"]['--PROCESS_NAME'] = job_name

            config = GlueJobConfig(
                job_name=job_name,
                executable=glue.JobExecutable.python_etl(
                    glue_version=glue.GlueVersion.V5_0,
                    python_version=glue.PythonVersion.THREE,
                    script=glue.Code.from_bucket(self.s3_artifacts_bucket.bucket_name, f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE_ANALYTICS}/{config['layer']}/{config['job_name']}.py")
                ),
                default_arguments=config["environment"],
                continuous_logging=glue.ContinuousLoggingProps(enabled=True),
                timeout=Duration.minutes(60),
                max_concurrent_runs=200
            )
            job_glue = self.builder.build_glue_job(config)

            # Grant permissions to the Glue job
            self.s3_artifact_bucket.grant_read(job_glue)
            self.s3_external_bucket.grant_read_write(job_glue)
            self.s3_stage_bucket.grant_read_write(job_glue)
            self.s3_analytics_bucket.grant_read_write(job_glue)

            # IAM policies for Glue job
            read_dynamodb_policy = iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "dynamodb:Scan"
                ],
                resources=["*"]
            )
            publish_sns_policy = iam.PolicyStatement(
                actions=POLICY_UTILS.SNS_PUBLIS_PERMISSIONS,
                resources=[
                    self.sns_failed_topic.topic_arn,
                ]
            )

            # Attach policies to the Glue job role
            job_glue.role.add_to_policy(read_dynamodb_policy)
            job_glue.role.add_to_policy(publish_sns_policy)

    def import_sns_topics(self):
        """Import an existing SNS topic"""
        self.sns_failed_topic = self.builder.import_sns_topic(self.PROJECT_CONFIG.app_config["topic_notifications"]["failed"])
        self.sns_success_topic = self.builder.import_sns_topic(self.PROJECT_CONFIG.app_config["topic_notifications"]["success"])
    
    def create_lambda_layers(self):
        """Create or reference required Lambda layers"""
        self.lambda_layer_pyodbc = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "LambdaPyodbcLayer",
            layer_version_arn=self.Layers.AWS_LAMBDA_LAYERS.get("layer_pyodbc")
        )
        
    def create_lambdas(self):
        """Create a lambda function definition for the Datalake Ingest BigMagic stack"""
        
        function_name = "get_data"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_ANALYTICS}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.minutes(10)
        )
        self.lambda_get_data = self.builder.build_lambda_function(lambda_config)
        
        function_name = "crawler"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE_ANALYTICS}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.minutes(10)
        )
        self.lambda_crawler = self.builder.build_lambda_function(lambda_config)
        
    def create_glue_jobs(self):
        """Create a job definition for the Datalake Ingest BigMagic stack"""
        self.__build_glue_job("dominio")
        self.__build_glue_job("comercial")

    def create_step_functions(self):
        """Create a Step Function definition for the Datalake Ingestion workflow"""
        LAMBDA_RETRY = 2
        GLUE_RETRY = 2

        # Process Table preparation task
        prepare_table = tasks.LambdaInvoke(
            self, "Prepare Table",
            lambda_function=self.lambda_prepare_table,
            result_path="$",
            output_path="$.Payload"
        )
        
        prepare_table.add_retry(
            errors=["Lambda.ClientExecutionTimeoutException", "Lambda.ServiceException", 
                    "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
            interval=Duration.seconds(2),
            max_attempts=LAMBDA_RETRY,
            backoff_rate=2
        )
        
        # Get endpoint Lambda task
        get_endpoint = tasks.LambdaInvoke(
            self, "get endpoint",
            lambda_function=self.lambda_get_endpoint,
            output_path="$.Payload"
        )
        
        get_endpoint.add_retry(
            errors=["Lambda.ClientExecutionTimeoutException", "Lambda.ServiceException", 
                    "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
            interval=Duration.seconds(2),
            max_attempts=LAMBDA_RETRY,
            backoff_rate=2
        )
        
        # Check Execute Raw choice state
        check_execute_raw = sfn.Choice(self, "Check Execute Raw")
        
        # Normalize output when execute_raw is false
        normalize_output_when_false = sfn.Pass(
            self, "normalize output when false",
            parameters={
                "raw_job_result": {
                    "JobRunId": "N/A-skipped",
                    "Status": "SKIPPED"
                },
                "dynamodb_key.$": "$.dynamodb_key",
                "process.$": "$.process",
                "execute_raw.$": "$.execute_raw"
            }
        )
        
        # Error handling for raw job
        error_raw_job = sfn.Pass(
            self, "error raw job",
            result_path="$.raw_job_error_result",
            output_path="$"
        )
        
        # Raw job Glue task
        raw_job = tasks.GlueStartJobRun(
            self, "raw job",
            glue_job_name=self.job_extract_data_bigmagic.job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--TABLE_NAME.$": "$.dynamodb_key.table"
            }),
            result_path="$.raw_job_result"
        )
        
        raw_job.add_retry(
            errors=["Glue.AWSGlueException", "Glue.ConcurrentRunsExceededException"],
            max_attempts=GLUE_RETRY,
            backoff_rate=5
        )
        
        raw_job.add_catch(
            errors=["States.TaskFailed"],
            handler=error_raw_job
        )
        
        # Stage job Glue task
        stage_job = tasks.GlueStartJobRun(
            self, "stage job",
            glue_job_name=self.job_light_transform.job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--JOB_NAME": self.job_light_transform.job_name,
                "--TABLE_NAME.$": "$.dynamodb_key.table"
            }),
            result_path="$.stage_job_result"
        )
        
        stage_job.add_retry(
            errors=["Glue.AWSGlueException", "Glue.ConcurrentRunsExceededException"],
            max_attempts=GLUE_RETRY,
            backoff_rate=5
        )
        
        # Standardize map output
        standardize_map_output = sfn.Pass(
            self, "standardize map output",
            result_path=None,
        )
        
        # Crawler job Glue task
        crawler_job = tasks.GlueStartJobRun(
            self, "crawler job",
            glue_job_name=self.job_crawler_stage.job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--JOB_NAME": self.job_crawler_stage.job_name,
                "--ENDPOINT.$": "$.endpoint",
                "--PROCESS_ID.$": "$.process_id"
            })
        )
        
        crawler_job.add_retry(
            errors=["States.ALL"],
            max_attempts=GLUE_RETRY,
            backoff_rate=5
        )
        
        # Define the workflow connections
        check_execute_raw.when(
            sfn.Condition.boolean_equals("$.execute_raw", False),
            normalize_output_when_false
        ).otherwise(raw_job)
        
        normalize_output_when_false.next(stage_job)
        raw_job.next(stage_job)
        error_raw_job.next(stage_job)
        stage_job.next(standardize_map_output)
        
        # Inner Map state for Process Table
        process_table_map = sfn.Map(
            self, "Process Table",
            max_concurrency=15,
            items_path="$.dynamodb_key",
            parameters={
                "dynamodb_key.$": "$$.Map.Item.Value",
                "process.$": "$.process",
                "execute_raw.$": "$.execute_raw"
            },
            result_selector={
                "result.$": "$[*]",
                "status": "COMPLETED"
            },
            result_path="$.query_results"
        )
        
        # Define the inner Map iterator
        process_table_map.iterator(check_execute_raw)
        
        # Connect to get_endpoint and crawler_job
        process_table_map.next(get_endpoint)
        get_endpoint.next(crawler_job)
        
        # Outer Map state for Process Table Group
        process_table_group_map = sfn.Map(
            self, "Process Table Group",
            max_concurrency=1,
            items_path="$.table_names",
            parameters={
                "dynamodb_key.$": "$$.Map.Item.Value",
                "process.$": "$.process",
                "execute_raw.$": "$.execute_raw"
            },
            result_path=None
        )
        
        # Define the outer Map iterator
        process_table_group_map.iterator(prepare_table.next(process_table_map))
         
        # Define the complete state machine
        config = StepFunctionConfig(
            name="light_transform_bigmagic",
            definition=process_table_group_map
        )
        
        self.state_function = self.builder.build_step_function(config)
