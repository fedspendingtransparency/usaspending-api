Running Spark Prototypes Locally
====

## Installing Dependencies

1. Get `pyenv` to manage app-specific Python versions
    - Get it from `brew` on Mac: (Note: probably best to do a `brew update` before this:
        - If you already have it, or are not sure, try upgrading: `brew upgrade pyenv` :warning: NOTE: _upgrading_ pyenv may be necessary to get a later version of Python running
        - If not, install: `brew install pyenv`
    - Add pyenv init to your `~/.bash_profile` file:

        ```bash
        # Adding pyenv and its shims to the shell to intercept commands like python, pip
        if command -v pyenv 1>/dev/null 2>&1; then
          eval "$(pyenv init --path)"
        fi
        ``` 
1. Download Docker Desktop Community (for Mac). 
    - It **must** be at version `2.5.0.0 (49427) - stable` (or above?), Engine: `Docker version 19.03.13`. 
        - _Bugs caused writes to minio running in a docker container to fail in earlier versions._
    - NOTE: Now trying `v3.5.2`, with `Engine v20.10.7` and `Compose v1.29.2` 
1. `Spark` will be pip-installed at the version pinned in `requirements.txt` when getting `pyspark`, and `Hadoop` jars are packaged within it.
    - NOTE: The _purpose_ of having Spark+Hadoop binaries (JARs) in your local python virtual environment is (1) for your editor/IDE to hava auto-complete against the PySpark API, (2) so you can spin up a pyspark shell and try things out quickly, and (3) so you _could_ use your local development machine to act the role of the Spark **Driver**, and invoke `spark-submit` to submit jobs to the dockerized Spark **Master** -- _however, introduction of the `spark-submit` container now takes over that role_
1. `make local-dev-setup` (_THERE SHOULD BE NO RED ERROR OUTPUT_)
    1. Will install the correct python version with `pyenv`
    1. Set up and (temporarily) activate a python virtual environment
    1. Install python required packages with `pip`
    1. Print out library and package versions
    1. You should check dependency versions printed out are as expected (can also be run standalone with `make check-dependencies`).
        1. `python -V && python3 -V` _should be what was installed with `pyenv`_
        1. `pip list` _should show what dependent or editable (development) packages have been installed_
        1. `pyspark --version` _will show the version of Spark bundled in the `pyspark` pip lib you installed._
        1. `Hadoop x.y.z` printed out will be teh version of Hadoop jars bundled in that pip-installed `pyspark` lib
## Steps to Run It

### 1. Update Environment Variables
1. Copy `.env.template` to your own `.env` file in the same root dir (it will be git-ignored)
1. Open `.env` in an editor and review values for existing variables -- mainly where data is stored (and persisted after restarts) locally for bound docker volumes
    - `POSTGRES_CLUSTER_DIR` -- directory where postgres database data exists or will be saved.
    - `ES_CLUSTER_DIR` -- directory where elasticsearch cluster node data exists or will be saved. 
    - `MINIO_DATA_DIR` -- directory that will be used as the root of a local "S3" object store, to store "S3 bucket" data locally (and persist it after restarts)
    - `SPARK_CLUSTER_DATA_DIR` -- directory that may contain data used by all nodes (history server, master, worker, driver) of a running Spark Cluster. _TBD how best to use this._
1. Create directories for all locations if they do not yet exist

### 2. Run Tests
Use this make rule to trigger `pytest`, and run all tests in the `/tests` top-level directory
```bash
make tests
```

### 3. Run Docker Services
1. Run the services using this command from the root (NOTE: Docker (at the RIGHT version -- see dependencies above) must be installed and running and Docker Compose must be installed)
    ```
    make docker-compose-up
    ```
    1. Runs Postgres 10.x on port `5432`
    2. Runs Elasticsearch 7.1.1 on port `9200`
    3. Runs MinIO (local S3 store) on port `9000`
    4. Runs Spark Master on port `7077` with a SparkUI on port `4040`
    5. Runs Spark Worker, with a SparkUI on port `4041`
    6. Runs Spark History Server, with a web UI port on `18080`
    7. Runs the spark-submit command's help in a container, and dies off.

There are other `make docker-compose*` rules that can be used. Look in the included `Makefile`. With many of these rules, you can provide `args="..."` at the end to append additional `docker-compose` options to the underlying call.
    
### 4. Run Scripts
See run instructions in the module-level docstring of a script, e.g. `./prototypes/spark_write_to_s3_delta.py`

_:warning: Keep mindful of relative hostname resolution when running things in docker. e.g. Spark executing in a docker worker, and trying to connect to the MinIO container URL will need to use the _`container_name`_ or _hostname_ of that container, not `localhost`._

- _TODO: It may be worth creating 2 distinct local env configs: `local` (`lcl`) and `local-docker` (`dkr`)_
    