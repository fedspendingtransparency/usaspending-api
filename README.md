# USASpending API

This is the API that will drive the new USAspending.gov. It is currently under active development and not stable. 

## Getting Started

This API is built on Python 3.3+, Django, and Django Rest Framework. 

Assumptions:

* You're able to install software on your local machine
* You have git installed on your machine and are able to clone code repositories from GitHub. If this isn't the case, the easiest way to get started is to install [GitHub Desktop](https://desktop.github.com/ "GitHub desktop"), available for Windows or Mac.
* You're familiar with opening a terminal on your machine and using the command line as needed.

### Install PostgreSQL

[PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) is an object-relational database management system (ORDBMS) with an emphasis on extensibility and standards-compliance.

1. Download the correct PostgreSQL installer for your operating system from [EnterpriseDB](http://www.enterprisedb.com/products-services-training/pgdownload) (we recommend PostgreSQL 9.4.x).
2. Run the installer. As you proceed through the installation wizard, note your choices for port number, username, and password. You will need those when creating the `settings.py` file.

More complete install documentation is available on the PostgreSQL [wiki](https://wiki.postgresql.org/wiki/Detailed_installation_guides).

**Note:** If you're setting up the project on Mac OSX, we recommend using [homebrew](http://brew.sh) to install PostgreSQL.

### Install Python and Create Virtual Environment

The API's backend components currently run on Python 3.x. These instructions will walk you through the process of installing Python and creating a Python-based virtual environment to house the API. A virtual environment will isolate the project and its libraries from those running on your local system and prevent potential conflicts.

If you already have a Python development environment on your machine and a preferred way of managing it, feel free to skip to the next section. We wrote the directions below for folks who don't have a Python environment up and running yet and need the quickest way to get started.

1. Install Python 3.x:
    * Windows and OSX users can download a 3.x Python installer here: [https://www.python.org/downloads/](https://www.python.org/downloads/ "Python installer downloads")
    * Linux users can install Python 3.x using their distribution's package manager.

2. Use pip to install virtualenv (pip is Python's package manager and is automatically installed with Python 3.x):

        pip install virtualenv
3. Use pip to install virtualenvwrapper:

        pip install virtualenvwrapper

4. Tell virtualenvwrapper where on your machine to create virtual environments and add it to your profile. This is a one-time virtualenvwrapper setup step, and the process varies by operating system. [This tutorial](http://newcoder.io/begin/setup-your-machine/ "Python: setting up your computer") covers setting up virtualenvwrapper on OSX, Linux, and Windows.
   For Windows users, there may be extra steps needed.  If you run into an error on the import-module step, move the "VirtualEnvWrapper" folder from C:/Python27/Lib/site-packages/Users/*username*/Documents/WindowsPowerShell/Modules/ to C:/Users/*username*/Documents/WindowsPowerShell/Modules/.  Next, in powershell run the command "set-executionpolicy unrestricted".  Finally, in the VirtualEnvWrapper directory, open the file "VirtualEnvWrapperTabExpansion.psm1" and change "Function:TabExpansion" to "Function:TabExpansion2" in line 12.

5. Create a virtual environment for the USAspending API. In this example we've named the environment *usaspending-api*, but you can call it anything:

        mkvirtualenv usaspending-api

    **Note:** If you're running multiple versions of Python on your machine, you can make sure your environment is running the correct Python version by pointing to a specific binary

        mkvirtualenv --python=[path to installed Python 3.x executable] usaspending-api

6. You should see some output that looks similar to the example below. Essentially, this command creates and activates a new virtualenv named `usaspending-api` with its own set of Python libraries.  Anything you pip install from this point forward will be installed into the *usaspending-api* environment rather than your machine's global Python environment. Your command line prompt indicates which (if any) virtualenv is active.

 **Note:** in the command below, replace `/usr/local/bin/python3.4` with the path to your local Python 3.x executable.

        $ mkvirtualenv --python=/usr/local/bin/python3.4 usaspending-api
        Running virtualenv with interpreter /usr/local/bin/python3.4
        New python executable in usaspending-api/bin/python3.4
        Also creating executable in usaspending-api/bin/python
        Installing setuptools, pip...done.

        (usaspending-api)$

7. This new environment will be active until you run the `deactivate` command. You can re-activate the environment again at any time by typing `workon usaspending-api`.

### USAspending API Backend

Decide where on your machine you want the USAspending API code to live. From the command line, navigate there and clone the USAspending API repository from GitHub to your local environment:

        $ git clone https://github.com/fedspendingtransparency/usaspending-api.git

Navigate to the USAspending API's main folder:

        $ cd usaspending-api


Install the dependencies.  

        $ pip install -r requirements.txt

Next, configure your local settings. The `settings.py` file will check for an environment variable named `DATABASE_URL`. You can export this variable on the command line, or use a library like [autoenv](https://github.com/kennethreitz/autoenv) to load your environment variables when you `cd` into that directory. 

Exporting your `DATABASE_URL` environment variable should look something like this:

        $ export DATABASE_URL='postgres://USER:PASSWORD@HOST:PORT/NAME'
        

To test your database connection, try running the migrations that come with the project to set up your tables.

        $ python manage.py migrate

You should see output that looks like this:

    (usaspending-api):~/usaspending-api$ ./manage.py migrate
    Operations to perform:
    Apply all migrations: admin, auth, awards, contenttypes, sessions
    Running migrations:
    Applying awards.0001_initial... OK

Once you've done that, you're ready to start the development server

    $ python manage.py runserver

This will run the application at `127.0.0.1:8000` by default. You can change that by passing the host and port number in to the `runserver` command.

you should see something like this:
    
    Performing system checks...
    System check identified no issues (0 silenced).
    September 02, 2016 - 01:46:02
    Django version 1.10.1, using settings 'usaspending_api.settings'
    Starting development server at http://127.0.0.1:8000/
    Quit the server with CONTROL-C.

Now, go to `http://localhost:8000/api/v1/awards/` to see the API! There aren't any other urls set up at the moment. 

## Public Domain License

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication.

All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.
