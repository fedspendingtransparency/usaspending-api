# Directory Purpose

Store one-off Python and SQL scripts used for data change operations.

# Mandatory header for all script files in this directory
```
Jira Ticket Number(s): <DEV-..., >
Expected CLI: $ <psql .... | python3 ....>
Purpose: <a sentence to paragraph on the changes this script performs and expected outcome>
```

# FAQ
1. Why store them in a repository?
    * While it might seem better to store these scripts in Jira tickets, it is easy to lose them or to not know they exist.
    * Additionally, it is frequency beneficial to have them in a repository for CI/CD pipelines to pull.
1. Why use this repository?
    * Often the changes performed by these scripts are tied to code changes.
1. What is the justification for a separate directory to hold all one-off scripts?
    * Reason #1: It helps devs know where the scripts are located.
    * Reason #2: It will help to prune script files no longer useful since they will all be located together.
1. Do Django management commands belong here?
    * No.
1. When should a new file be placed here?
    * Any script which is written for a specific job and not expected to be used again can be placed here.
1. When should a file be moved out of this directory?
    * When the script is routinely used by operations or developers.
1. When should a script be deleted?
    * When the core developer team decided the script no longer holds value.
    * Example #1: a script would alter the contents of a table which no longer exists
    * Example #2: a script functionality is replaced by another operations script