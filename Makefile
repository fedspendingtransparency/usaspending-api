#### DEFAULTS ##########################################################################################################
#### Boilerplate Makefile setup
MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -ec -o pipefail
.DEFAULT_GOAL := help
.DELETE_ON_ERROR:
.SUFFIXES:


#### INCLUDES ##########################################################################################################
#### Includes of other Makefiles, or files that declare environment variables (e.g. .env)
# include .env _should_ allow vars to be defined here
# However be careful of vars referencing other vars. or vals with $ in it (e.g. passwords), which would need escaping

env_file_exists := $(wildcard .env)
ifneq ($(strip ${env_file_exists}),)
include .env
endif

#### VARS ##############################################################################################################
#### Variables used in this Makefile.
#### Uppercased are environment vars, or make-specific vars. All others should be lower-snake-case

# default ENV_CODE to lcl if not set
ENV_CODE ?= lcl
# default version if not set in .env or an env var
PYTHON_VERSION ?= 3.10.12
venv_name := usaspending-api
docker_compose_file := docker-compose.yml
dockerfile_for_spark := Dockerfile.spark
# Root directories under which python (namespace) packages start, for all python code in this project
src_root_paths = "."

#### RULES #############################################################################################################
#### Rules defining file targets that need to be made, or PHONY targets, which don't actually produce a file
#### Reminder: The name of non-PHONY targets needs to be the name of a file on disk, or it will always be invoked
#### NOTE: Most rules here deal with project-setup
####       Rules orchestrating project workloads are in the included Makefile

.PHONY: help
help: ## print this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: printvars
printvars: ## Print the Environment variables present in calls to make, plus variables defined in the executed Makefile
	@$(info ==== Makefile Variables ====)
	@$(info )
	@$(foreach V,$(sort $(.VARIABLES)), \
		$(if $(filter-out environment% default automatic, \
		$(origin $V)),$(info $V=$($V) ($(value $V)))))
	@printf "\n==== Environment Variables ====\n\n"
	@printenv

.python-version: ## Attempt to setup python using pyenv
	@if ! command -v pyenv &> /dev/null; then \
		echo "WARNING: pyenv could not be found. Install pyenv to get a virtual env running with the compatible python version: ${PYTHON_VERSION}. Will fallback to using system python3."; \
	else \
	  	set -x; \
	  	echo "pyenv setting python version to ${PYTHON_VERSION}"; \
  		pyenv install -s ${PYTHON_VERSION}; \
  		pyenv local ${PYTHON_VERSION}; \
  		python3 -V; \
  		if [ "$$(python3 -V)" != "Python ${PYTHON_VERSION}" ]; then \
  			echo "ERROR: pyenv was not able to set local python version to ${PYTHON_VERSION}"; \
  			exit 1; \
  		fi; \
	fi;

.venv: ## Ensure a virtual environment is established at .venv
	@( \
		set -x; \
		test -d .venv || python3 -m venv .venv/${venv_name}; \
	)

.PHONY: requirements-dev
upgrade :=  #unset it
requirements-dev: .venv ## Install pip packages in dev virtual env. Add upgrade=true to upgrade required packages to newest version (can be lengthy)
	# Because this depends on .venv, the virtual env should exist to activate
	# This will "activate" the virtual env only for the duration of the scripts in the parens-scope
	# Then when this make rule recipe is complete, the virtual env will be dropped
	# But it IS there and populated
	# Must _manually_ reactivate the virtual env to interact with it on the command line
	@( \
		source .venv/${venv_name}/bin/activate; \
		echo "virtual env at .venv/${venv_name} activated (temporarily)"; \
		pip install $$(cat requirements/requirements-dev.txt | grep 'pip=='); \
		src_roots=(${src_root_paths}); \
		for src_root in "$${src_roots[@]}"; do \
			pip install ${if ${upgrade},--upgrade,} --editable "$${src_root}[dev]"; \
		done; \
	)

.ivy2: ## Ensure user has a ~/.ivy2 dir, which will be bound to in a docker container volume to save on dependency downloads
	@mkdir -p ~/.ivy2

.PHONY: activate
activate: ## Spit out the command to run to activate the virtual env, since you can't do it within a make shell process. Use this like: source $(make activate)
	@echo ".venv/${venv_name}/bin/activate"

.PHONY: local-dev-setup
local-dev-setup: .python-version requirements-dev check-dependencies .ivy2 ## Setup python, virtual environment, and pip dependencies, then check version info

.PHONY: check-dependencies
check-dependencies: ## Prints out the versions of dependencies in use
	@printf "\n==== [PYTHON VERSIONS] ====\n\n"
	@echo "python -> $$(python -V) ... python3 -> $$(python3 -V)"
	@printf "\n==== [PIP PACKAGE VERSIONS] ====\n\n"
	@source .venv/${venv_name}/bin/activate && pip list
	@printf "\n==== [SPARK VERSION] ====\n\n"
	@source .venv/${venv_name}/bin/activate && pyspark --version
	@printf "\n==== [HADOOP VERSION] ====\n\n"
	@source .venv/${venv_name}/bin/activate && python3 -c "from pyspark.sql import SparkSession; \
spark = spark = SparkSession.builder.getOrCreate(); \
print('Hadoop ' + spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion());"

.PHONY: env-code
env-code:  ## Print the value of ENV_CODE environment variable
	@echo ${ENV_CODE}

.PHONY: test-dbs
createdb :=  #unset it
test-dbs:  ## Trigger the setup of multiple test DBs that can be reused with pytest --numprocesses. Add createdb=true to force (re-)creation of Test DBs rather than reuse.
	pytest ${if ${createdb},--create-db,} --reuse-db --numprocesses=auto --no-cov --disable-warnings -rP -vvv --capture=no --log-cli-level=WARNING --show-capture=log 2> /dev/null 'usaspending_api/tests/integration/test_setup_of_test_dbs.py::test_trigger_test_db_setup'

.PHONY: test-spark-deps
test-spark-deps:  ## Trigger a singular test in one pytest session that does nothing but cause Maven dependencies to be downloaded and cached through Ivy; reduces contention when parallel spark builds need the depdencies
	pytest --no-cov --disable-warnings -r=fEs --verbosity=3 'usaspending_api/tests/integration/test_setup_of_spark_dependencies.py::test_preload_spark_jars'

.PHONY: tests
tests: local-dev-setup test-dbs test-spark-deps ## Run automated unit/integration tests. Configured for useful logging. add args="..." to append additional pytest args
	pytest --failed-first --reuse-db --numprocesses=auto --dist=worksteal -rP -vv --capture=no --show-capture=log 2> /dev/null ${args}

.PHONY: tests-failed
tests-failed: local-dev-setup test-dbs test-spark-deps ## Re-run only automated unit/integration tests that failed on the previous run. Configured for verbose logging to get more detail on failures. logging. add args="..." to append additional pytest args
	pytest --last-failed --reuse-db --numprocesses=auto --dist=worksteal -rP -vvv ${args}

.PHONY: confirm-clean-all
no-prompt := 'false'
dry-run := 'false'
confirm-clean-all:  ## Guard to prompt for confirmation before aggressive clean
ifeq ($(strip ${no-prompt}),'false')
ifeq ($(strip ${dry-run}),'false')
	@echo -n "This will remove any untracked/uncommitted source files or files in the working directory. Consider backing up any files in your custom setup. To see what files would be removed, re-run with dry-run=true. Continue? [y/N] " && read ans && [ $${ans:-N} = y ]
endif
endif

.PHONY: clean-all
dry-run := 'false'
clean-all: confirm-clean-all  ## Remove all tmp artifacts and artifacts created as part of local dev env setup. To avoid prompt (e.g. in script) call like: make clean-all no-prompt=true. To only see what WOULD be deleted, include dry-run=true
ifeq ($(strip ${dry-run}),'false')
	rm -f .python-version
	rm -rf .venv
	@git clean -xfd --exclude='\.env' --exclude='\.envrc' --exclude='\.idea/' --exclude='spark-warehouse/' --exclude='\.vscode/'
	deactivate || true
	#if command -v deactivate &> /dev/null; then deactivate; fi;
else  # this is a dry-run, spit out what would be removed
	@printf "Would remove .python-version\nWould remove .venv\n"
	@git clean --dry-run -xfd --exclude='\.env' --exclude='\.envrc' --exclude='\.idea/' --exclude='spark-warehouse/'
endif


.PHONY: docker-compose
docker-compose: ## Run an arbitrary docker compose command by passing in the Docker Compose profiles in the "profiles" variable, and args in the "args" variable
	# NOTE: The .env file is used to provide environment variable values that replace variables in the compose file
	#       Because the .env file does not live in the same place as the compose file, we have to tell compose explicitly
	#       where it is with "--project_directory". Since this is called from the root Makefile, using ./ points to the dir
	#       of that Makefile
	docker compose ${profiles} --project-directory . --file ${docker_compose_file} ${args}

.PHONY: docker-compose-config
docker-compose-config:  ## Show config and vars expanded, which will be used in docker compose
	# NOTE: The .env file is used to provide environment variable values that replace varialbes in the compose file
	#       Because the .env file does not live in the same place as the compose file, we have to tell compose explicitly
	#       where it is with "--project_directory". Since this is called from teh root Makefile, using ./ points to the dir
	#       of that Makefile
	docker compose --project-directory . --file ${docker_compose_file} config ${args}

.PHONY: docker-compose-up-usaspending
docker-compose-up-usaspending: ## Deploy containerized version of this app on the local machine using docker compose
	# To 'up' a single docker compose service, pass it in the args var, e.g.: make deploy-docker args=my-service
	# NOTE: [See NOTE in docker compose rule about .env file]
	docker compose --profile usaspending --project-directory . --file ${docker_compose_file} up ${args}

.PHONY: docker-compose-up-s3
docker-compose-up-s3: ## Deploy minio container on the local machine using docker compose, which acts as a look-alike AWS S3 service
	# NOTE: [See NOTE in docker compose rule about .env file]
	echo "docker compose --profile s3 --project-directory . --file ${docker_compose_file} up ${args}"
	docker compose --profile s3 --project-directory . --file ${docker_compose_file} up ${args}

.PHONY: docker-compose-up-spark
docker-compose-up-spark: ## Deploy containerized version of spark cluster infrastructure on the local machine using docker compose
	# NOTE: [See NOTE in docker compose rule about .env file]
	docker compose --profile spark --project-directory . --file ${docker_compose_file} up ${args}

.PHONY: docker-compose-run
docker-compose-run: ## Use docker compose run <args> to run one or more Docker Compose services with options
	# NOTE: [See NOTE in docker compose rule about .env file]
	docker compose ${profiles} --project-directory . --file ${docker_compose_file} run ${args}

.PHONY: docker-compose-down
docker-compose-down: ## Run docker compose down to bring down services listed in the compose file
	# NOTE: [See NOTE in docker compose rule about .env file]
	docker compose --project-directory . --file ${docker_compose_file} down ${args}

.PHONY: docker-build-spark
docker-build-spark: ## Run docker build to build a base container image for spark, hadoop, and python installed
	# NOTE: [See NOTE in above docker compose rule about .env file]
	echo "docker build --tag spark-base --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args} --file ${dockerfile_for_spark} $$(dirname ${dockerfile_for_spark})"
	docker build --tag spark-base --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args} --file ${dockerfile_for_spark} $$(dirname ${dockerfile_for_spark})

.PHONY: docker-compose-build
docker-compose-build: ## Ensure ALL services in the docker-compose.yaml file have an image built for them according to their build: key
	# NOTE: This *may* creates a compose-specific image name IF an image: YAML key does not specify the image name to be used as
	#       a tag when compose has to build the image.
	#       If no image key is specified, then be aware that:
	#       While building and tagging the spark-base image can be done, docker compose will _NOT USE_ that image at runtime,
	#       but look for an image with its custom tag. It may use cached layers of that image when doing its build,
	#       but it will create a _differently named_ image: the image name is always going to be <project>_<service>,
	#       where project defaults to the directory name you're in. Therefore you MUST always run this command (or the manual version of it)
	#       anytime you want services run with Docker Compose to accommodate recent changes in the image (e.g. python package dependency changes)
	# NOTE: [See NOTE in above docker compose rule about .env file]
	echo "docker compose --profile usaspending --project-directory . --file ${docker_compose_file} build --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args}"
	docker compose --profile usaspending --project-directory . --file ${docker_compose_file} build --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args}

.PHONY: docker-compose-build-spark
docker-compose-build-spark: ## See: docker-compose-build rule. This builds just the subset of spark services.
	# NOTE: [See NOTE in above docker compose rule about .env file]=
	echo "docker compose --profile spark --project-directory . --file ${docker_compose_file} build --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args}"
	docker compose --profile spark --project-directory . --file ${docker_compose_file} build --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args}

.PHONY: docker-compose-spark-submit
docker-compose-spark-submit: ## Run spark-submit from within local docker containerized infrastructure (which must be running first). Set params with django_command="..."
	docker compose --profile=spark --project-directory . --file ${docker_compose_file} run \
		-e MINIO_HOST=minio \
		-e COMPONENT_NAME='${django_command}${python_script}' \
		-e DATABASE_URL=${DATABASE_URL} \
	spark-submit \
	--driver-memory "2g" \
	--packages org.postgresql:postgresql:42.2.23,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-hive_2.12:3.5.0 \
	${if ${python_script}, \
		${python_script}, \
		/project/manage.py ${django_command} \
	}

.PHONY: localhost-spark-submit
localhost-spark-submit: ## Run spark-submit from with localhost as the driver and worker (single node). Set params with django_command="..."
	SPARK_LOCAL_IP=127.0.0.1 \
	spark-submit \
	--driver-memory "2g" \
	--packages org.postgresql:postgresql:42.2.23,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-hive_2.12:3.5.0 \
	${if ${python_script}, \
		${python_script}, \
		manage.py ${django_command} \
	}

.PHONY: pyspark-shell
pyspark-shell: ## Launch a local pyspark REPL shell with all of the packages and spark config pre-set
	SPARK_LOCAL_IP=127.0.0.1 pyspark \
	--packages org.postgresql:postgresql:42.2.23,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
	--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	--conf spark.hadoop.fs.s3a.endpoint=localhost:${MINIO_PORT} \
	--conf spark.hadoop.fs.s3a.access.key=usaspending \
	--conf spark.hadoop.fs.s3a.secret.key=usaspender \
	--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
	--conf spark.hadoop.fs.s3a.path.style.access=true \
	--conf spark.sql.catalogImplementation=hive \
	--conf spark.sql.warehouse.dir='$(PWD)/spark-warehouse' \
	--conf spark.hadoop.javax.jdo.option.ConnectionURL='jdbc:derby:;databaseName=$(PWD)/spark-warehouse/metastore_db;create=true'
