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
import pandas as pd

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
    
    def __load_data_config(self, file_path: str):
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
            '--S3_PATH_ARTIFACTS_CSV': f"s3a://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE_CSV}",
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

        file_path = f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CONFIG}/{layer}.csv"
        
        configs = self.__load_data_config(file_path)
        for config in configs:
            job_name=f"{config["layer"]}-{config["procedure"]}"
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
        self.__build_glue_job("domain")
        self.__build_glue_job("analytics")

    def create_step_functions(self):
        """Create Step Function definitions for the Datalake Analytics Commercial workflow"""
    
        # 1. Crear las máquinas de estado base para validación concurrente
        self._create_base_state_machines()
        
        # 2. Crear las máquinas de estado por capas (dominio, comercial)
        self._create_layer_state_machines()
        
        # 3. Crear la máquina de estado de orquestación principal
        self._create_orchestration_state_machine()

    def _create_base_state_machines(self):
        """Create base state machines for job validation and execution"""
        
        # Configuración base para máquinas de estado
        base_config = StepFunctionConfig(
            name="analytics_base",
            definition=self._build_base_definition()
        )
        
        self.state_machine_base = self.builder.build_step_function(base_config)
        
        # Máquina de estado específica para Redshift
        redshift_config = StepFunctionConfig(
            name="analytics_base_redshift", 
            definition=self._build_redshift_base_definition()
        )
        
        self.state_machine_base_redshift = self.builder.build_step_function(redshift_config)

    def _create_layer_state_machines(self):
        """Create state machines for each analytical layer"""
        
        # Obtener configuración de jobs por capa desde CSV
        layer_jobs = self._load_jobs_configuration()
        
        # Crear máquina de estado para capa de dominio
        if 'dominio' in layer_jobs:
            dominio_config = StepFunctionConfig(
                name="analytics_dominio",
                definition=self._build_layer_definition(
                    layer='dominio',
                    jobs=layer_jobs['dominio'],
                    crawler_name=self.crawler_dominio.crawler_name if hasattr(self, 'crawler_dominio') else None
                )
            )
            self.state_machine_dominio = self.builder.build_step_function(dominio_config)
        
        # Crear máquina de estado para capa comercial
        if 'comercial' in layer_jobs:
            comercial_config = StepFunctionConfig(
                name="analytics_comercial", 
                definition=self._build_layer_definition(
                    layer='comercial',
                    jobs=layer_jobs['comercial'],
                    crawler_name=self.crawler_comercial.crawler_name if hasattr(self, 'crawler_comercial') else None
                )
            )
            self.state_machine_comercial = self.builder.build_step_function(comercial_config)

    def _create_orchestration_state_machine(self):
        """Create the main orchestration state machine"""
        
        orchestration_config = StepFunctionConfig(
            name="analytics_orchestrate",
            definition=self._build_orchestration_definition()
        )
        
        self.state_machine_analytics = self.builder.build_step_function(orchestration_config)

    def _build_base_definition(self):
        """Build the base state machine definition for job validation"""
        
        # Task: Validar ejecución concurrente
        validate_execution = tasks.LambdaInvoke(
            self, "ValidateExecution",
            lambda_function=self.lambda_get_data,
            payload=sfn.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "own_process_id.$": "$.own_process_id",
                "job_name.$": "$.job_name",
                "COD_PAIS.$": "$.COD_PAIS",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "redshift.$": "$.redshift"
            }),
            result_path="$.validation_result"
        )
        
        # Choice: Determinar si ejecutar o esperar
        execution_choice = sfn.Choice(self, "ExecutionChoice")
        
        # Wait state para concurrencia
        wait_state = sfn.Wait(
            self, "WaitForConcurrency",
            time=sfn.WaitTime.duration(Duration.seconds(60))
        )
        
        # Task: Ejecutar job de Glue
        execute_glue_job = tasks.GlueStartJobRun(
            self, "ExecuteGlueJob",
            glue_job_name=sfn.JsonPath.string_at("$.job_name"),
            arguments=sfn.TaskInput.from_object({
                "--COD_PAIS.$": "$.COD_PAIS",
                "--INSTANCIAS.$": "$.INSTANCIAS",
                "--PERIODS.$": "$.PERIODS"
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB
        )
        
        # Error handling
        error_notification = tasks.SnsPublish(
            self, "NotifyError",
            topic=self.sns_failed_topic,
            message=sfn.TaskInput.from_object({
                "error": "Job execution failed",
                "job_name.$": "$.job_name",
                "timestamp.$": "$$.State.EnteredTime"
            })
        )
        
        # Definir flujo
        validate_execution.next(
            execution_choice
            .when(
                sfn.Condition.boolean_equals("$.validation_result.Payload.wait", True),
                wait_state.next(validate_execution)
            )
            .when(
                sfn.Condition.boolean_equals("$.validation_result.Payload.error", True),
                error_notification
            )
            .when(
                sfn.Condition.boolean_equals("$.validation_result.Payload.ready", True),
                execute_glue_job
            )
            .otherwise(sfn.Succeed(self, "SkipExecution"))
        )
        
        return validate_execution

    def _build_layer_definition(self, layer: str, jobs: dict, crawler_name: str = None):
        """Build state machine definition for a specific analytical layer"""
        
        # Crear estados paralelos por orden de ejecución
        current_state = None
        initial_state = None
        
        # Procesar jobs agrupados por orden de ejecución
        for order in sorted(jobs.keys()):
            # Crear estado paralelo para este orden
            parallel_state = sfn.Parallel(
                self, f"{layer.title()}Order{order}",
                result_path="$.execution_results"
            )
            
            # Agregar cada job como rama paralela
            for job_config in jobs[order]:
                job_execution = tasks.StepFunctionsStartExecution(
                    self, f"Execute{job_config['job_name'].title()}",
                    state_machine=self.state_machine_base,
                    input=sfn.TaskInput.from_object({
                        "exe_process_id.$": "$.exe_process_id",
                        "own_process_id": job_config['process_id'],
                        "job_name": job_config['glue_job_name'],
                        "COD_PAIS.$": "$.COD_PAIS",
                        "INSTANCIAS.$": "$.INSTANCIAS",
                        "PERIODS": job_config['periods'],
                        "redshift": False
                    }),
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB
                )
                parallel_state.branch(job_execution)
            
            # Conectar estados secuencialmente
            if current_state is None:
                initial_state = parallel_state
            else:
                current_state.next(parallel_state)
            
            current_state = parallel_state
        
        # Agregar crawler al final si existe
        if crawler_name and current_state:
            crawler_task = tasks.LambdaInvoke(
                self, f"RunCrawler{layer.title()}",
                lambda_function=self.lambda_crawler,
                payload=sfn.TaskInput.from_object({
                    "CRAWLER_NAME": crawler_name
                })
            )
            
            # Wait loop para el crawler
            crawler_wait = sfn.Wait(
                self, f"WaitCrawler{layer.title()}",
                time=sfn.WaitTime.duration(Duration.seconds(60))
            )
            
            crawler_choice = sfn.Choice(self, f"CheckCrawler{layer.title()}")
            
            start_crawler = tasks.CallAwsService(
                self, f"StartCrawler{layer.title()}",
                service="glue",
                action="startCrawler",
                parameters={"Name": crawler_name},
                iam_resources=["*"]
            )
            
            crawler_task.next(
                crawler_choice
                .when(
                    sfn.Condition.boolean_equals("$.Payload.wait", True),
                    crawler_wait.next(crawler_task)
                )
                .otherwise(start_crawler)
            )
            
            current_state.next(crawler_task)
        
        return initial_state or sfn.Succeed(self, f"NoJobs{layer.title()}")

    def _build_orchestration_definition(self):
        """Build the main orchestration state machine definition"""
        
        # Ejecutar dominio primero
        execute_dominio = tasks.StepFunctionsStartExecution(
            self, "ExecuteDominio",
            state_machine=self.state_machine_dominio,
            input=sfn.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "COD_PAIS.$": "$.COD_PAIS",
                "INSTANCIAS.$": "$.INSTANCIAS"
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB
        )
        
        # Ejecutar comercial en paralelo (si existe)
        parallel_execution = sfn.Parallel(
            self, "ExecuteAnalyticalLayers",
            result_path="$.layer_results"
        )
        
        if hasattr(self, 'state_machine_comercial'):
            execute_comercial = tasks.StepFunctionsStartExecution(
                self, "ExecuteComercial",
                state_machine=self.state_machine_comercial,
                input=sfn.TaskInput.from_object({
                    "exe_process_id.$": "$.exe_process_id",
                    "COD_PAIS.$": "$.COD_PAIS",
                    "INSTANCIAS.$": "$.INSTANCIAS"
                }),
                integration_pattern=sfn.IntegrationPattern.RUN_JOB
            )
            parallel_execution.branch(execute_comercial)
        
        # Notificación de éxito
        success_notification = tasks.SnsPublish(
            self, "NotifySuccess",
            topic=self.sns_success_topic,
            message=sfn.TaskInput.from_object({
                "status": "SUCCESS",
                "message": "Analytics pipeline completed successfully",
                "timestamp.$": "$$.State.EnteredTime"
            })
        )
        
        # Construir flujo principal
        definition = execute_dominio
        
        if parallel_execution.branches:
            definition = definition.next(parallel_execution)
        
        definition = definition.next(success_notification)
        
        return definition

    def _load_jobs_configuration(self) -> dict:
        """Load Glue jobs configuration from CSV files"""
        import pandas as pd
        
        jobs_by_layer = {}
        
        # Cargar configuración de dominio
        try:
            df_dominio = pd.read_csv(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CONFIG}/domain.csv", delimiter=';')
            jobs_by_layer['dominio'] = self._process_jobs_config(df_dominio, 'dominio')
        except FileNotFoundError:
            logger.warning("Domain configuration file not found")
        
        # Cargar configuración comercial
        try:
            df_comercial = pd.read_csv(f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CONFIG}/analytics.csv", delimiter=';')
            jobs_by_layer['comercial'] = self._process_jobs_config(df_comercial, 'comercial')
        except FileNotFoundError:
            logger.warning("Analytics configuration file not found")
        
        return jobs_by_layer

    def _process_jobs_config(self, df: pd.DataFrame, layer: str) -> dict:
        """Process jobs configuration DataFrame into structured format"""
        jobs_by_order = {}
        
        for _, row in df.iterrows():
            if row['layer'] != layer:
                continue
                
            order = str(row['exe_order'])
            if order not in jobs_by_order:
                jobs_by_order[order] = []
            
            # Construir nombre del job de Glue usando la convención
            glue_job_name = f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value}-{self.PROJECT_CONFIG.project_name}-{layer}-{row['procedure']}-job"
            
            jobs_by_order[order].append({
                'job_name': row['procedure'],
                'glue_job_name': glue_job_name,
                'process_id': str(row['process_id']),
                'periods': row['periods'],
                'num_workers': row['num_workers']
            })
        
        return jobs_by_order

    def _build_redshift_base_definition(self):
        """Build base definition for Redshift operations"""
        import aws_cdk.aws_stepfunctions as sfn
        import aws_cdk.aws_stepfunctions_tasks as tasks
        
        # Similar a _build_base_definition pero específico para Redshift
        # con parámetros adicionales como LOAD_PROCESS y ORIGIN
        
        validate_execution = tasks.LambdaInvoke(
            self, "ValidateRedshiftExecution",
            lambda_function=self.lambda_get_data,
            payload=sfn.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "job_name.$": "$.job_name",
                "COD_PAIS.$": "$.COD_PAIS",
                "INSTANCIAS.$": "$.INSTANCIAS",
                "redshift": True
            }),
            result_path="$.validation_result"
        )
        
        execute_redshift_job = tasks.GlueStartJobRun(
            self, "ExecuteRedshiftJob",
            glue_job_name=sfn.JsonPath.string_at("$.job_name"),
            arguments=sfn.TaskInput.from_object({
                "--COD_PAIS.$": "$.COD_PAIS",
                "--LOAD_PROCESS.$": "$.exe_process_id",
                "--ORIGIN.$": "$.ORIGIN",
                "--INSTANCIAS.$": "$.INSTANCIAS"
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB
        )
        
        choice = sfn.Choice(self, "RedshiftExecutionChoice")
        
        validate_execution.next(
            choice
            .when(
                sfn.Condition.boolean_equals("$.validation_result.Payload.ready", True),
                execute_redshift_job
            )
            .otherwise(sfn.Succeed(self, "SkipRedshiftExecution"))
        )
        
        return validate_execution
