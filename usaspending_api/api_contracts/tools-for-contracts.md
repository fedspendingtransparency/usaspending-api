
# API Contract Testing

## For Everyone

You should be using Node 6.11.0

### Contracts

Contracts are stored in the `/contracts` directory. They must be written in [API Blueprint](https://apiblueprint.org/) but saved with a `.md` extension.

## For Testers and Automation

### Generating API Documentation

To generate API documentation, run:

```
npm run docs
```

Documentation will be output as `.html` files in the `/docs` directory, mirroring the same folder structure as `/contracts`.

`/docs` is currently gitignored.

### Generating JSON Schema

To generate JSON Schemas from the contracts, run:

```
npm run schema
```

A single `schema.json` file will be generated in `/schemas` (currently gitignored). This JSON file will contain a object: each API endpoint will have a single key in the object. The value associated with that key will be an array. This array will contain one JSON Schema per API response that was specified in the API Blueprint files.

### Automated Dev API Compliance Testing

To validate that the dev API is in compliance with the API contracts, run:

```
npm run provider
```

This will run all the API contracts against the hosted development API environment.

## For Developers

### Writing API Contracts

When writing API contracts, it can be helpful to have real-time validation that your API Blueprint syntax is correct. You can use [Aglio](https://github.com/danielgtaylor/aglio) to render the API documentation in real-time on each change; Aglio will also output any parsing errors it runs into.

Install Aglio globally in your CLI:

```
npm install -g aglio
```

Run the following command:

```
aglio -i ./contracts/[path to specific contract file].md -s -p [port number of your choice]
```

Example:

```
aglio -i ./contracts/state/StateProfile.md -s -p 4000
```

Now go to `http://localhost:[port]` in your browser.

### Running Local Compliance Tests

To run compliance tests locally and against the server of your choice (ie., an API developer's local host), install the [Dredd](https://dredd.readthedocs.io/en/latest/quickstart.html) CLI tool.

Install Dredd:

```
npm install -g dredd
```

Call Dredd with either a glob (to match against multiple contracts) or a file path (to match against a specific contract) and an endpoint to test.

Testing all API contracts:

```
dredd ./contracts/**/*.md http://localhost:8000
```

This will run all the API contracts against a local server on port 8000.

Testing a specific API contract:

```
dredd ./contracts/state/StateProfile.md http://localhost:8000
```

**Remember:** Your developed endpoint must pass *all* API contracts at PR time.

### Generating Mock APIs

When API development occurs concurrently with API integration development, you'll need to mock the upcoming API. You can use [Drakov](https://github.com/Aconex/drakov) to do this.

Run:
```
npm run mock
```

This will spin up a mock API server at `http://localhost:5000` for every API contract ending in `.md` found within `contracts`.

In `GlobalConstants_dev.js` in the `usaspending-website` code, change the `API` value to `http://localhost:5000/api/`.

#### Mocking Specific API Blueprints

Install Drakov globally:

```
npm install -g drakov
```

Run the following command:

```
drakov -f ./contracts/state/StateProfile.md -p 5000
```

This will mock the `StateProfile.md` files in the `contracts/state` directory at `http://localhost:5000`.

**Important Note:** Drakov's globbing only returns one file per directory, so you may need to combine your contracts into a single file for this to work. There also appear to be a cap on the number of files it will traverse in different directories.

Modify the `-f` glob if you only want to mock a subset of the routes. Remember that you may need to mock multiple routes in order to get certain pages to load (some API calls may be sequential).

Modify the `-p` value to change the server port.

In `GlobalConstants_dev.js` in the `usaspending-website` code, change the `API` value to `http://localhost:5000/api/` (or whatever you have set the port to).
