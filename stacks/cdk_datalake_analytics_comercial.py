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
    aws_glue_alpha as glue_alpha,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks
)
import aws_cdk
from constructs import Construct
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.models.configs import *
from aje_cdk_libs.constants.environments import Environments
from aje_cdk_libs.constants.policies import PolicyUtils
from constants.paths import Paths
from constants.layers import Layers
import os
from dotenv import load_dotenv
import urllib.parse
from aje_cdk_libs.constants.project_config import ProjectConfig
import pandas as pd
import logging
import json

# Al inicio de tu clase o archivo
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
        
        # Inicializar estructuras de datos AQUÍ, antes de cualquier otro método
        self.glue_jobs = {}
        self.cr_targets = {}
        self.state_machine_order_list = {}
        self.relation_process = {}  # ← Esta línea faltaba

        self.import_s3_buckets()
        self.import_dynamodb_tables()
        self.import_sns_topics()
        self.create_iam_roles()
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
    
    def create_iam_roles(self):
        """Crear roles IAM necesarios"""
        
        # Role para Glue Jobs
        glue_job_role_config = RoleConfig(
            role_name="glue-analytics-job-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
            inline_policies={
                "S3AccessPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.join_permissions(
                                PolicyUtils.S3_READ,
                                PolicyUtils.S3_WRITE
                            ),
                            resources=[
                                self.s3_artifacts_bucket.bucket_arn,
                                f"{self.s3_artifacts_bucket.bucket_arn}/*",
                                self.s3_external_bucket.bucket_arn,
                                f"{self.s3_external_bucket.bucket_arn}/*",
                                self.s3_stage_bucket.bucket_arn,
                                f"{self.s3_stage_bucket.bucket_arn}/*",
                                self.s3_analytics_bucket.bucket_arn,
                                f"{self.s3_analytics_bucket.bucket_arn}/*"
                            ]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.DYNAMODB_READ,
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.SNS_PUBLISH,
                            resources=[self.sns_failed_topic.topic_arn]
                        )
                    ]
                )
            }
        )
        self.glue_job_role = self.builder.build_role(glue_job_role_config)
        
        # Role para Glue Crawler
        glue_crawler_role_config = RoleConfig(
            role_name="glue-analytics-crawler-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLakeFormationDataAdmin")
            ]
        )
        self.glue_crawler_role = self.builder.build_role(glue_crawler_role_config)
        
        # Roles para Step Functions
        sf_role_config = RoleConfig(
            role_name="stepfunctions-analytics-role",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies={
                "StepFunctionPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "glue:StartJobRun",
                                "glue:GetJobRun",
                                "glue:GetJobRuns",
                                "glue:BatchStopJobRun",
                                "glue:StartCrawler",
                                "glue:GetCrawler",
                                "glue:GetCrawlers",
                                "glue:StopCrawler"
                            ],
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "lambda:InvokeFunction"
                            ],
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "states:StartExecution",
                                "states:StopExecution",
                                "states:DescribeExecution"
                            ],
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=PolicyUtils.SNS_PUBLISH,
                            resources=[
                                self.sns_failed_topic.topic_arn,
                                self.sns_success_topic.topic_arn
                            ]
                        )
                    ]
                )
            }
        )
        self.step_function_role = self.builder.build_role(sf_role_config)
        
        # Role para Lambda functions
        lambda_role_config = RoleConfig(
            role_name="lambda-analytics-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
            inline_policies={
                "GlueAccessPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "glue:GetJobRuns",
                                "glue:StartCrawler",
                                "glue:GetCrawler",
                                "glue:GetCrawlers",
                                "glue:StopCrawler"
                            ],
                            resources=["*"]
                        )
                    ]
                )
            }
        )
        self.lambda_role = self.builder.build_role(lambda_role_config)

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
        """Create Glue job definitions for the Datalake Analytics stack"""
        
        # 1. Cargar configuración desde archivos separados por capa
        jobs_config = self._load_jobs_from_separate_files()
        
        # 2. Crear jobs, databases y crawlers
        #self.glue_jobs = {}
        #self.cr_targets = {}
        #self.state_machine_order_list = {}
        #self.relation_process = {}
        
        # 3. Procesar cada capa
        for layer, jobs_data in jobs_config.items():
            if jobs_data:  # Solo procesar si hay datos
                self._build_glue_jobs_for_layer(layer, jobs_data)
                logger.info(f"Created {len(jobs_data)} Glue jobs for layer: {layer}")
        
        # 4. Crear jobs especiales de Redshift
        self._create_redshift_jobs()
        
        # 5. Crear databases y crawlers
        self._create_glue_databases_and_crawlers()

    def _load_jobs_from_separate_files(self) -> dict:
        """Load Glue jobs configuration from separate CSV files (domain.csv and analytics.csv)"""
        import pandas as pd
        
        jobs_by_layer = {}
        
        # Mapeo de archivos a capas
        file_layer_mapping = {
            'domain.csv': 'domain',
            'analytics.csv': 'analytics'
        }
        
        for filename, layer_name in file_layer_mapping.items():
            csv_path = f"{self.Paths.LOCAL_ARTIFACTS_GLUE_CONFIG}/{filename}"
            
            try:
                logger.info(f"Loading jobs configuration from: {csv_path}")
                df = pd.read_csv(csv_path, delimiter=';')
                
                if not df.empty:
                    jobs_data = df.to_dict('records')
                    jobs_by_layer[layer_name] = jobs_data
                    
                    # Procesar relaciones de proceso mientras cargamos
                    self._process_job_relations(jobs_data, layer_name)
                    
                    logger.info(f"Loaded {len(jobs_data)} jobs for layer '{layer_name}' from {filename}")
                else:
                    logger.warning(f"No jobs found in {filename}")
                    jobs_by_layer[layer_name] = []
                    
            except FileNotFoundError:
                logger.warning(f"Configuration file not found: {csv_path}")
                jobs_by_layer[layer_name] = []
            except Exception as e:
                logger.error(f"Error loading {filename}: {str(e)}")
                jobs_by_layer[layer_name] = []
        
        return jobs_by_layer

    def _process_job_relations(self, jobs_data: list, layer_name: str):
        """Process job relations for Step Function arguments"""
        
        for job_data in jobs_data:
            process_id = str(job_data.get('process_id', '10'))
            
            if process_id not in self.relation_process:
                self.relation_process[process_id] = []
            
            self.relation_process[process_id].append({
                'table': job_data['procedure'],
                'periods': job_data.get('periods', 2),
                'layer': layer_name  # Usar el nombre de capa mapeado
            })

    def _build_glue_jobs_for_layer(self, layer: str, jobs_config: list):
        """Build Glue jobs for a specific layer"""
        
        # Inicializar estructuras si no existen
        if layer not in self.glue_jobs:
            self.glue_jobs[layer] = {}
        
        if layer not in self.cr_targets:
            self.cr_targets[layer] = []
        
        if layer not in self.state_machine_order_list:
            self.state_machine_order_list[layer] = {}
        
        # Crear argumentos base para jobs
        jobs_args = self._build_jobs_arguments()
        
        for job_data in jobs_config:
            procedure_name = job_data['procedure']
            
            # Validar que el script existe
            script_path = f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/{layer}/{procedure_name}.py"
            
            try:
                # Crear configuración del job
                job_config = GlueJobConfig(
                    job_name=f"{self.BUSINESS_PROCESS}_{layer}_{procedure_name}",
                    executable=glue_alpha.JobExecutable.python_etl(  # ← glue_alpha para executable
                        glue_version=self._get_glue_version(job_data.get('glue_version', '4')),
                        python_version=glue_alpha.PythonVersion.THREE,
                        script=glue_alpha.Code.from_bucket(
                            self.s3_artifacts_bucket, 
                            script_path
                        )
                    ),
                    default_arguments={
                        **jobs_args,
                        '--PROCESS_NAME': f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-{layer}-{procedure_name}-job",
                        '--FLOW_NAME': layer,
                        '--PERIODS': str(job_data.get('periods', 2))
                    },
                    worker_type=self._get_worker_type(job_data.get('worker_type', 'G.1X')),
                    worker_count=job_data.get('num_workers', 2),
                    continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
                    timeout=Duration.hours(1),
                    max_concurrent_runs=100
                )
                
                # Crear el job usando ResourceBuilder
                glue_job = self.builder.build_glue_job(job_config)
                
                # Agregar permisos necesarios
                self._grant_job_permissions(glue_job)
                
                # Guardar referencia del job
                self.glue_jobs[layer][procedure_name] = glue_job
                
                # Crear target para crawler (ubicación S3 donde escribe el job)
                s3_target_path = f"{self.TEAM}/{self.BUSINESS_PROCESS}/{layer}/{procedure_name}/"
                self.cr_targets[layer].append(
                    aws_cdk.aws_glue.CfnCrawler.DeltaTargetProperty(  # ← glue (no glue_alpha) para CfnCrawler
                        create_native_delta_table=False,
                        delta_tables=[f"s3://{self.s3_analytics_bucket.bucket_name}/{s3_target_path}"],
                        write_manifest=True
                    )
                )
                
                # Organizar para Step Functions si tiene orden de ejecución
                exe_order = job_data.get('exe_order', 0)
                if exe_order > 0:
                    self._add_to_execution_order(layer, job_data, glue_job.job_name)
                    
                logger.info(f"Created Glue job: {glue_job.job_name}")
                
            except Exception as e:
                logger.error(f"Failed to create Glue job for {layer}/{procedure_name}: {str(e)}")
                continue

    def _add_to_execution_order(self, layer: str, job_data: dict, job_name: str):
        """Add job to Step Function execution order"""
        
        exe_order = job_data.get('exe_order', 0)
        order_key = str(exe_order)
        
        if order_key not in self.state_machine_order_list[layer]:
            self.state_machine_order_list[layer][order_key] = []
        
        self.state_machine_order_list[layer][order_key].append({
            'table': job_data['procedure'],
            'process_id': str(job_data.get('process_id', '10')),
            'job_name': job_name
        })

    def _grant_job_permissions(self, glue_job):
        """Grant necessary permissions to Glue job"""
        
        # Permisos S3
        self.s3_artifacts_bucket.grant_read(glue_job)
        self.s3_external_bucket.grant_read_write(glue_job)
        self.s3_stage_bucket.grant_read_write(glue_job)
        self.s3_analytics_bucket.grant_read_write(glue_job)
        
        # Permisos DynamoDB
        read_dynamodb_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["dynamodb:Scan"],
            resources=["*"]
        )
        glue_job.role.add_to_policy(read_dynamodb_policy)
        
        # Permisos SNS
        publish_sns_policy = iam.PolicyStatement(
            actions=["sns:Publish"],
            resources=[self.sns_failed_topic.topic_arn]
        )
        glue_job.role.add_to_policy(publish_sns_policy)

    def _build_jobs_arguments(self) -> dict:
        """Build common arguments for all Glue jobs"""
        
        base_args = {
            '--extra-py-files': f's3://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE_LAYER}/common_jobs_functions.py',
            '--S3_PATH_STG': f"s3a://{self.s3_stage_bucket.bucket_name}/{self.TEAM}/",
            '--S3_PATH_ANALYTICS': f"s3a://{self.s3_analytics_bucket.bucket_name}/{self.TEAM}/{self.BUSINESS_PROCESS}/",
            '--S3_PATH_EXTERNAL': f"s3a://{self.s3_external_bucket.bucket_name}/{self.TEAM}/",
            '--S3_PATH_ARTIFACTS': f"s3a://{self.s3_landing_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE}/",
            '--S3_PATH_ARTIFACTS_CSV': f"s3a://{self.s3_artifacts_bucket.bucket_name}/{self.Paths.AWS_ARTIFACTS_GLUE_CSV}",
            '--REGION_NAME': self.PROJECT_CONFIG.region_name,
            '--DYNAMODB_DATABASE_NAME': self.dynamodb_credentials_table.table_name,
            '--DYNAMODB_LOGS_TABLE': self.dynamodb_logs_table.table_name,
            '--COD_PAIS': 'PE',
            '--INSTANCIAS': 'PE',
            '--COD_SUBPROCESO': '0',
            '--ERROR_TOPIC_ARN': self.sns_failed_topic.topic_arn,
            '--PROJECT_NAME': self.PROJECT_CONFIG.app_config["team"],
            '--datalake-formats': "delta",
            '--enable-continuous-log-filter': "true",
            '--LOAD_PROCESS': '10',
            '--ORIGIN': 'AJE'
        }
        
        # Agregar relaciones de proceso como argumentos (equivalente al --P{key})
        for key, value in self.relation_process.items():
            base_args[f'--P{key}'] = json.dumps(value)
        
        return base_args

    def _create_redshift_jobs(self):
        """Create special Glue jobs for Redshift operations"""
        
        jobs_args = self._build_jobs_arguments()
        
        # Job para cargar datos finales a Redshift
        redshift_config = GlueJobConfig(
            job_name=f"{self.BUSINESS_PROCESS}_load_to_redshift",
            executable=glue_alpha.JobExecutable.python_etl(
                glue_version=glue_alpha.GlueVersion.V4_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_bucket(
                    self.s3_artifacts_bucket,
                    f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/redshift/load_to_redshift.py"
                )
            ),
            default_arguments=jobs_args,
            worker_type=glue_alpha.WorkerType.G_1_X,
            worker_count=2,
            connections=[self.glue_redshift_connection] if hasattr(self, 'glue_redshift_connection') else [],
            continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
            timeout=Duration.hours(2),
            max_concurrent_runs=10
        )
        
        self.load_to_redshift_job = self.builder.build_glue_job(redshift_config)
        
        # Job para cargar datos de stage a Redshift
        stage_redshift_config = GlueJobConfig(
            job_name=f"{self.BUSINESS_PROCESS}_load_stage_to_redshift",
            executable=glue_alpha.JobExecutable.python_etl(
                glue_version=glue_alpha.GlueVersion.V4_0,
                python_version=glue_alpha.PythonVersion.THREE,
                script=glue_alpha.Code.from_bucket(
                    self.s3_artifacts_bucket,
                    f"{self.Paths.AWS_ARTIFACTS_GLUE_CODE}/redshift/load_stage_to_redshift.py"
                )
            ),
            default_arguments=jobs_args,
            worker_type=glue_alpha.WorkerType.G_1_X,
            worker_count=2,
            connections=[self.glue_redshift_connection] if hasattr(self, 'glue_redshift_connection') else [],
            continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
            timeout=Duration.hours(2),
            max_concurrent_runs=10
        )
        
        self.load_stage_to_redshift_job = self.builder.build_glue_job(stage_redshift_config)

    def _create_glue_databases_and_crawlers(self):
        """Create Glue databases and crawlers for each layer"""
        
        self.databases_layers = {}
        self.crawlers_layers = {}
        
        for layer_key, targets in self.cr_targets.items():
            if not targets:  # Skip if no targets
                continue
                
            # Crear database
            database_name = f"{self.TEAM}_{self.BUSINESS_PROCESS}_{layer_key}"
            
            database = aws_cdk.aws_glue.CfnDatabase(  # ← glue (no glue_alpha)
                self, f"Database{layer_key.title()}",
                catalog_id=self.account,
                database_input=aws_cdk.aws_glue.CfnDatabase.DatabaseInputProperty(
                    name=database_name
                )
            )
            
            self.databases_layers[layer_key] = database
            
            # Crear crawler
            crawler_name = f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.BUSINESS_PROCESS}-{self.PROJECT_CONFIG.project_name}-{layer_key}-crawler"
            
            crawler = aws_cdk.aws_glue.CfnCrawler(  # ← glue (no glue_alpha)
                self, f"Crawler{layer_key.title()}",
                name=crawler_name,
                database_name=database_name,
                role=self.glue_crawler_role.role_arn,
                targets=aws_cdk.aws_glue.CfnCrawler.TargetsProperty(delta_targets=targets),
                schema_change_policy=aws_cdk.aws_glue.CfnCrawler.SchemaChangePolicyProperty(
                    update_behavior="UPDATE_IN_DATABASE",
                    delete_behavior="DEPRECATE_IN_DATABASE"
                )
            )
            
            self.crawlers_layers[layer_key] = crawler
            
            logger.info(f"Created database and crawler for layer: {layer_key}")

    # Métodos auxiliares que ya tienes
    def _get_glue_version(self, version_str: str) -> glue_alpha.GlueVersion:
        """Convert version string to GlueVersion enum"""
        version_map = {
            '3': glue_alpha.GlueVersion.V3_0,
            '4': glue_alpha.GlueVersion.V4_0,
            '5': glue_alpha.GlueVersion.V4_0
        }
        return version_map.get(str(version_str), glue_alpha.GlueVersion.V4_0)

    def _get_worker_type(self, worker_str: str) -> glue_alpha.WorkerType:
        """Convert worker string to WorkerType enum"""
        worker_map = {
            'G.1X': glue_alpha.WorkerType.G_1_X,
            'G.2X': glue_alpha.WorkerType.G_2_X,
            # Nota: G_4_X y G_8_X no existen en aws-glue-alpha
        }
        return worker_map.get(worker_str, glue_alpha.WorkerType.G_1_X)

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
            name=f"{self.BUSINESS_PROCESS}_analytics_base",
            definition_body=sfn.DefinitionBody.from_chainable(self._build_base_definition())
            #definition=self._build_base_definition()
        )
        
        self.state_machine_base = self.builder.build_step_function(base_config)
        
        # Máquina de estado específica para Redshift
        redshift_config = StepFunctionConfig(
            name=f"{self.BUSINESS_PROCESS}_analytics_base_redshift", 
            definition_body=sfn.DefinitionBody.from_chainable(self._build_redshift_base_definition())
            #definition=self._build_redshift_base_definition()
        )
        
        self.state_machine_base_redshift = self.builder.build_step_function(redshift_config)

    def _create_layer_state_machines(self):
        """Create state machines for each analytical layer"""
        
        # Obtener configuración de jobs por capa desde CSV
        layer_jobs = self._load_jobs_from_separate_files()
        
        # Crear máquina de estado para capa de dominio
        if 'domain' in layer_jobs:
            crawler_name = None
            if 'domain' in self.crawlers_layers:
                crawler_name = self.crawlers_layers['domain'].name
                
            definition = self._build_layer_definition(
                layer='dominio',
                jobs=layer_jobs['domain'],
                crawler_name=crawler_name
            )
            dominio_config = StepFunctionConfig(
                name=f"{self.BUSINESS_PROCESS}_analytics_domain",                
                definition_body=sfn.DefinitionBody.from_chainable(definition)
            )
            self.state_machine_domain = self.builder.build_step_function(dominio_config)
        
        # Crear máquina de estado para capa comercial/analytics
        if 'analytics' in layer_jobs:
            crawler_name = None
            if 'analytics' in self.crawlers_layers:
                crawler_name = self.crawlers_layers['analytics'].name
                
            definition = self._build_layer_definition(
                layer='comercial',
                jobs=layer_jobs['analytics'],
                crawler_name=crawler_name
            )
            comercial_config = StepFunctionConfig(
                name=f"{self.BUSINESS_PROCESS}_analytics_analytics",                 
                definition_body=sfn.DefinitionBody.from_chainable(definition)
            )
            self.state_machine_analytics = self.builder.build_step_function(comercial_config)

    def _create_orchestration_state_machine(self):
        """Create the main orchestration state machine"""
        
        orchestration_config = StepFunctionConfig(
            name=f"{self.BUSINESS_PROCESS}_analytics_orchestrate",
            definition_body=sfn.DefinitionBody.from_chainable(self._build_orchestration_definition())
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

    def _build_layer_definition(self, layer: str, jobs: list, crawler_name: str = None):
        """Build state machine definition for a specific analytical layer"""
        
        # Agrupar jobs por orden de ejecución
        jobs_by_order = {}
        for job_data in jobs:
            exe_order = str(job_data.get('exe_order', 0))
            if exe_order not in jobs_by_order:
                jobs_by_order[exe_order] = []
            
            # Construir nombre del job de Glue usando la convención
            glue_job_name = f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value.lower()}-{self.PROJECT_CONFIG.project_name}-{self.BUSINESS_PROCESS}_{layer}_{job_data['procedure']}-job"
            
            jobs_by_order[exe_order].append({
                'job_name': job_data['procedure'],
                'glue_job_name': glue_job_name,
                'process_id': str(job_data.get('process_id', '10')),
                'periods': job_data.get('periods', 2)
            })
        
        # Crear estados paralelos por orden de ejecución
        current_state = None
        initial_state = None
        
        # Procesar jobs agrupados por orden de ejecución
        for order in sorted(jobs_by_order.keys()):
            # Solo procesar órdenes mayores a 0
            if int(order) <= 0:
                continue
                
            # Crear estado paralelo para este orden
            parallel_state = sfn.Parallel(
                self, f"{layer.title()}Order{order}",
                result_path="$.execution_results"
            )
            
            # Agregar cada job como rama paralela
            for job_config in jobs_by_order[order]:
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
            state_machine=self.state_machine_domain,
            input=sfn.TaskInput.from_object({
                "exe_process_id.$": "$.exe_process_id",
                "COD_PAIS.$": "$.COD_PAIS",
                "INSTANCIAS.$": "$.INSTANCIAS"
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB
        )
        
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
        
        # Verificar si existe la máquina de estado de analytics/comercial
        if hasattr(self, 'state_machine_analytics') and self.state_machine_analytics:
            # Solo crear parallel si hay máquinas de estado adicionales
            parallel_execution = sfn.Parallel(
                self, "ExecuteAnalyticalLayers",
                result_path="$.layer_results"
            )
            
            execute_comercial = tasks.StepFunctionsStartExecution(
                self, "ExecuteComercial",
                state_machine=self.state_machine_analytics,
                input=sfn.TaskInput.from_object({
                    "exe_process_id.$": "$.exe_process_id",
                    "COD_PAIS.$": "$.COD_PAIS",
                    "INSTANCIAS.$": "$.INSTANCIAS"
                }),
                integration_pattern=sfn.IntegrationPattern.RUN_JOB
            )
            parallel_execution.branch(execute_comercial)
            
            # Construir flujo con parallel
            definition = execute_dominio.next(parallel_execution).next(success_notification)
        else:
            # Flujo simple sin parallel si no hay máquinas adicionales
            definition = execute_dominio.next(success_notification)
        
        return definition

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
            glue_job_name = f"{self.PROJECT_CONFIG.enterprise}-{self.PROJECT_CONFIG.environment.value}-{self.PROJECT_CONFIG.project_name}-{self.BUSINESS_PROCESS}_{layer}_{row['procedure']}-job"
            
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
