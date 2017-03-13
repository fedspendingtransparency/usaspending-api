<ul class="nav nav-stacked" id="sidebar">
  <li><a href="#introduction">Introduction</a></li>
  <li><a href="#endpoint-overview">Endpoint Overview</a></li>
  <li><a href="#get-vs-post">GET vs POST Requests</a></li>
  <li><a href="#filtering">Filtering</a></li>
  <li><a href="#autocompletes">Autocomplete Requests</a></li>
  <li><a href="#aggregation">Aggregation</a></li>
  <li><a href="#other">Other Information</a></li>
</ul>
[//]: # (Begin Content)

# Introduction <a name="introduction"></a>

Welcome to the USASpending API tutorial. Over the next few sections, we will discuss the different methods for accessing the API, how to filter the data, how to use autocomplete endpoints, and how to find more information.

# Endpoint Overview <a name="endpoint-overview"></a>

The USASpending API supports a number of endpoints. Generally, these are broken into a few groups:

* Data endpoints - Return a number of records corresponding to that endpoint's data
* Autocomplete endpoints - Support autocomplete queries for constructing user interfaces based upon API data
* Aggregation endpoints - Support various aggregation methods (summation, counting, etc.) on a set of data

In the next sections, we will be discussing _Data Endpoints_. For information on Autocomplete and Aggregation endpoints, please view their respective sections using the table of contents.

Each endpoint accesses a different subset of the total universe of the data stored on USASpending. For example, the endpoint `/v1/api/awards/` accesses information at the award<span title="An award is comprised of multiple actions (known as transactions)"><sup>?</sup></span> level; whereas `/v1/api/transactions/` accesses information on individual transactions<span title="A transaction represents a specific contract or assistance action"><sup>?</sup></span>.

For a comprehensive list of endpoints and their data, please see the USASpending API Data Dictionary.

# GET vs POST requests <a name="get-vs-post"></a>

# Filtering <a name="filtering"></a>

# Autocomplete Requests <a name="autocompletes"></a>

# Aggregation Requests <a name="aggregation"></a>

# Other Information  <a name="other"></a>
