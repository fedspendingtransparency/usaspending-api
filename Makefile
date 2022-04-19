#### DEFAULTS ##########################################################################################################
#### Boilerplate Makefile setup
MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail
.DEFAULT_GOAL := all
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
docker_compose_file := ./docker-compose.yaml
dockerfile_for_spark := ./docker/spark/Dockerfile
# Root directories under which python (namespace) packages start, for all python code in this project
src_root_paths = "usaspending_api"

#### RULES #############################################################################################################
#### Rules defining file targets that need to be made, or PHONY targets, which don't actually produce a file
#### Reminder: The name of non-PHONY targets needs to be the name of a file on disk, or it will always be invoked
#### NOTE: Most rules here deal with project-setup
####       Rules orchestrating project workloads are in the included Makefile

# Print the Environment variables present in calls to make, plus variables defined in the executed Makefile
.PHONY: printvars
printvars:
	@$(info ==== Makefile Variables ====)
	@$(info )
	@$(foreach V,$(sort $(.VARIABLES)), \
		$(if $(filter-out environment% default automatic, \
		$(origin $V)),$(info $V=$($V) ($(value $V)))))
	@printf "\n==== Environment Variables ====\n\n"
	@printenv

# Attempt to setup python using us pyenv
.python-version:
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

# This will "activate" the virtual env only for the duration of the scripts in the parens-scope
# Then when this make rule recipe is complete, the virtual env will be dropped
# But it IS there and populated
# Must _manually_ reactivate the virtual env to interact with it on the command line
.venv:
	@( \
		set -x; \
		test -d .venv || python3 -m venv .venv/${venv_name}; \
		source .venv/${venv_name}/bin/activate; \
		echo "virtual env at .venv/${venv_name} activated (temporarily)"; \
		src_roots=(${src_root_paths}); \
		for src_root in "$${src_roots[@]}"; do \
			pip install -e "$${src_root}[dev]"; \
		done; \
	)

# Spit out the command to run to activate the virtual env, since you can't do it within a make shell process
# Use this like: source $(make activate)
.PHONY: activate
activate:
	@echo ".venv/${venv_name}/bin/activate"

# Setup python, virtual environment, and pip dependencies, then check version info
.PHONY: local-dev-setup
local-dev-setup: .python-version .venv check-dependencies

# Prints out the versions of dependencies in use
.PHONY: check-dependencies
check-dependencies:
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

# Print the value of ENV_CODE environment variable
.PHONY: env-code
env-code:
	@echo ${ENV_CODE}

.PHONY: tests
tests:
	@pytest -rP -vv

# Guard to prompt for confirmation before aggressive clean
.PHONY: confirm-clean-all
no-prompt := 'false'
confirm-clean-all:
ifeq ($(strip ${no-prompt}),'false')
	@echo -n "This will remove any untracked/uncommitted source files. Continue? [y/N] " && read ans && [ $${ans:-N} = y ]
endif

# Remove all tmp artifacts and artifacts created as part of local dev env setup
# To avoid prompt (e.g. in script) call like: make clean-all no-prompt=true
.PHONY: clean-all
clean-all: confirm-clean-all
	rm -f .python-version
	rm -rf .venv
	@git clean -xfd --exclude='\.env'
	if command -v deactivate &> /dev/null; then deactivate; fi;

# Run an arbitrary docker-compose command by passing in the args in the "args" variable
# NOTE: The .env file is used to provide environment variable values that replace variables in the compose file
#       Because the .env file does not live in the same place as the compose file, we have to tell compose explicitly
#       where it is with "--project_directory". Since this is called from the root Makefile, using ./ points to the dir
#       of that Makefile
.PHONY: docker-compose
docker-compose:
	docker-compose --project-directory ./ --file ${docker_compose_file} ${args}

# Show config and vars expanded, which will be used in docker-compose
# NOTE: The .env file is used to provide environment variable values that replace varialbes in the compose file
#       Because the .env file does not live in the same place as the compose file, we have to tell compose explicitly
#       where it is with "--project_directory". Since this is called from teh root Makefile, using ./ points to the dir
#       of that Makefile
.PHONY: docker-compose-config
docker-compose-config:
	docker-compose --project-directory ./ --file ${docker_compose_file} config

# Deploy containerized version of this app on the local machine using docker-compose
# To 'up' a single docker-compose service, pass it in the args var, e.g.: make deploy-docker args=my-service
# NOTE: [See NOTE in docker-compose rule about .env file]
.PHONY: docker-compose-up
docker-compose-up:
	docker-compose --project-directory ./ --file ${docker_compose_file} up ${args}

# Use docker-compose run <args> to run one or more Docker Compose services with options
# NOTE: [See NOTE in docker-compose rule about .env file]
.PHONY: docker-compose-run
docker-compose-run:
	docker-compose --project-directory ./ --file ${docker_compose_file} run ${args}

# Run docker-compose down to bring down services listed in the compose file
# NOTE: [See NOTE in docker-compose rule about .env file]
.PHONY: docker-compose-down
docker-compose-down:
	docker-compose --project-directory ./ --file ${docker_compose_file} down

# Run docker build to build a base container image for spark, hadoop, and python installed
# NOTE: [See NOTE in above docker-compose rule about .env file]
.PHONY: docker-build-spark
docker-build-spark:
	echo "docker build --tag spark-base --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args} $$(dirname ${dockerfile_for_spark})"
	docker build --tag spark-base --build-arg PROJECT_LOG_DIR=${PROJECT_LOG_DIR} ${args} $$(dirname ${dockerfile_for_spark})
