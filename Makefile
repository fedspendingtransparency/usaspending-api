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
ENV_CODE ?= lcl  # default ENV_CODE to lcl if not set
python_version := 3.7.3
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
		echo "WARNING: pyenv could not be found. Install pyenv to get a virtual env running with the compatible python version: ${python_version}. Will fallback to using system python3."; \
	else \
	  	set -x; \
	  	echo "pyenv setting python version to ${python_version}"; \
  		pyenv install -s ${python_version}; \
  		pyenv local ${python_version}; \
  		python3 -V; \
  		if [ "$$(python3 -V)" != "Python ${python_version}" ]; then \
  			echo "ERROR: pyenv was not able to set local python version to ${python_version}"; \
  			exit 1; \
  		fi; \
	fi;

.venv: ## Ensure a virtual environment is established at .venv
	# This will "activate" the virtual env only for the duration of the scripts in the parens-scope
	# Then when this make rule recipe is complete, the virtual env will be dropped
	# But it IS there and populated
	# Must _manually_ reactivate the virtual env to interact with it on the command line
	@( \
		set -x; \
		test -d .venv || python3 -m venv .venv/${venv_name}; \
		source .venv/${venv_name}/bin/activate; \
		echo "virtual env at .venv/${venv_name} activated (temporarily)"; \
		src_roots=(${src_root_paths}); \
		for src_root in "$${src_roots[@]}"; do \
			pip install --editable "$${src_root}[dev]"; \
		done; \
	)

.ivy2: ## Ensure user has a ~/.ivy2 dir, which will be bound to in a docker container volume to save on dependency downloads
	@mkdir -p ~/.ivy2

.PHONY: activate
activate: ## Spit out the command to run to activate the virtual env, since you can't do it within a make shell process. Use this like: source $(make activate)
	@echo ".venv/${venv_name}/bin/activate"

.PHONY: local-dev-setup
local-dev-setup: .python-version .venv check-dependencies .ivy2 ## Setup python, virtual environment, and pip dependencies, then check version info

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

.PHONY: tests
tests:  ## Run automated unit/integration tests
	@pytest -rP -vv

.PHONY: confirm-clean-all
no-prompt := 'false'
confirm-clean-all:  ## Guard to prompt for confirmation before aggressive clean
ifeq ($(strip ${no-prompt}),'false')
	@echo -n "This will remove any untracked/uncommitted source files. Continue? [y/N] " && read ans && [ $${ans:-N} = y ]
endif

.PHONY: clean-all
clean-all: confirm-clean-all  ## Remove all tmp artifacts and artifacts created as part of local dev env setup. To avoid prompt (e.g. in script) call like: make clean-all no-prompt=true
	rm -f .python-version
	rm -rf .venv
	@git clean -xfd --exclude='\.env'
	if command -v deactivate &> /dev/null; then deactivate; fi;

.PHONY: docker-compose
docker-compose: ## Run an arbitrary docker-compose command by passing in the Docker Compose profiles in the "profiles" variable, and args in the "args" variable
	# NOTE: The .env file is used to provide environment variable values that replace variables in the compose file
	#       Because the .env file does not live in the same place as the compose file, we have to tell compose explicitly
	#       where it is with "--project_directory". Since this is called from the root Makefile, using ./ points to the dir
	#       of that Makefile
	docker-compose ${profiles} --project-directory . --file ${docker_compose_file} ${args}

.PHONY: docker-compose-config
docker-compose-config:  ## Show config and vars expanded, which will be used in docker-compose
	# NOTE: The .env file is used to provide environment variable values that replace varialbes in the compose file
	#       Because the .env file does not live in the same place as the compose file, we have to tell compose explicitly
	#       where it is with "--project_directory". Since this is called from teh root Makefile, using ./ points to the dir
	#       of that Makefile
	docker-compose --project-directory . --file ${docker_compose_file} config ${args}

.PHONY: docker-compose-up-usaspending
docker-compose-up-usaspending: ## Deploy containerized version of this app on the local machine using docker-compose
	# To 'up' a single docker-compose service, pass it in the args var, e.g.: make deploy-docker args=my-service
	# NOTE: [See NOTE in docker-compose rule about .env file]
	docker-compose --profile usaspending --project-directory . --file ${docker_compose_file} up ${args}

.PHONY: docker-compose-up-s3
docker-compose-up-s3: ## Deploy minio container on the local machine using docker-compose, which acts as a look-alike AWS S3 service
	# NOTE: [See NOTE in docker-compose rule about .env file]
	echo "docker-compose --profile s3 --project-directory . --file ${docker_compose_file} up ${args}"
	docker-compose --profile s3 --project-directory . --file ${docker_compose_file} up ${args}

.PHONY: docker-compose-up-spark
docker-compose-up-spark: ## Deploy containerized version of spark cluster infrastructure on the local machine using docker-compose
	# NOTE: [See NOTE in docker-compose rule about .env file]
	docker-compose --profile spark --project-directory . --file ${docker_compose_file} up ${args}

.PHONY: docker-compose-run
docker-compose-run: ## Use docker-compose run <args> to run one or more Docker Compose services with options
	# NOTE: [See NOTE in docker-compose rule about .env file]
	docker-compose ${profiles} --project-directory . --file ${docker_compose_file} run ${args}

.PHONY: docker-compose-down
docker-compose-down: ## Run docker-compose down to bring down services listed in the compose file
	# NOTE: [See NOTE in docker-compose rule about .env file]
	docker-compose --project-directory . --file ${docker_compose_file} down ${args}

.PHONY: docker-build-spark
docker-build-spark: ## Run docker build to build a base container image for spark, hadoop, and python installed
	# NOTE: [See NOTE in above docker-compose rule about .env file]
	echo "docker build --tag spark-base --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args} --file ${dockerfile_for_spark} $$(dirname ${dockerfile_for_spark})"
	docker build --tag spark-base --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args} --file ${dockerfile_for_spark} $$(dirname ${dockerfile_for_spark})

.PHONY: docker-compose-build
docker-compose-build: ## Ensure ALL services in the docker-compose.yaml file have an image built for them according to their build: key
	# NOTE: This *may* creates a compose-specific image name IF an image: YAML key does not specify the image name to be used as
	#       a tag when compose has to build the image.
	#       If no image key is specified, then be aware that:
	#       While building and tagging the spark-base image can be done, docker-compose will _NOT USE_ that image at runtime,
	#       but look for an image with its custom tag. It may use cached layers of that image when doing its build,
	#       but it will create a _differently named_ image: the image name is always going to be <project>_<service>,
	#       where project defaults to the directory name you're in. Therefore you MUST always run this command (or the manual version of it)
	#       anytime you want services run with Docker Compose to accommodate recent changes in the image (e.g. python package dependency changes)
	# NOTE: [See NOTE in above docker-compose rule about .env file]
	echo "docker-compose --profile usaspending --project-directory . --file ${docker_compose_file} build --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args}"
	docker-compose --profile usaspending --project-directory . --file ${docker_compose_file} build --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args}

.PHONY: docker-compose-build-spark
docker-compose-build-spark: ## See: docker-compose-build rule. This builds just the subset of spark services.
	# NOTE: [See NOTE in above docker-compose rule about .env file]=
	echo "docker-compose --profile spark --project-directory . --file ${docker_compose_file} build --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args}"
	docker-compose --profile spark --project-directory . --file ${docker_compose_file} build --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args}
