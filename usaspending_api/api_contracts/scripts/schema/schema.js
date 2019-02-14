const path = require('path');
const fse = require('fs-extra');
const drafter = require('drafter');

const { fetchContracts, readContract } = require('../common/common.js');

const writeSchemaFile = (output) => new Promise((resolve, reject) => {
    // change the file name
    const fileName = 'schemas.json';
    const destPath = path.resolve(process.cwd(), './schemas', fileName);
    fse.outputFile(destPath, JSON.stringify(output, null, 4), 'utf8', (err) => {
        if (err) {
            return reject(err);
        }
        resolve(destPath);
    })
});

const parseSchemas = (refract) => {
    return refract.reduce((output, element) => {
        if (element.element === 'asset' && element.attributes.contentType && element.attributes.contentType === 'application/schema+json') {
            output.push(JSON.parse(element.content));
        }
        else if (element.content && Array.isArray(element.content)) {
            return parseSchemas(element.content);
        }
        return output;
    }, []);
};

const parseEndpoints = (refract, parsed) => {
    // console.log(refract);
    return refract.reduce((output, element) => {
        if (element.element === 'resource') {
            const endpoint = element.attributes.href;
            const schemas = parseSchemas(element.content);

            if (output[endpoint]) {
                return Object.assign({}, output, {
                    [endpoint]: output[endpoint].concat(schemas)
                });
            }
            else {
                return Object.assign({}, output, {
                    [endpoint]: schemas
                });
            }
        }
        else if (element.content && Array.isArray(element.content)) {
            return parseEndpoints(element.content, output);
        }
        return output;
    }, Object.assign({}, parsed));
};

const buildSchemaFromContract = (contract) => new Promise((resolve, reject) => {
    drafter.parse(contract, {}, (err, res) => {
        if (!err) {
            resolve(parseEndpoints(res.content, {}));
        }
        else {
            reject(err);
        }
    });
});

const generateSingleSchema = (contractPath) => {
    return readContract(contractPath)
        .then((contract) => {
            return buildSchemaFromContract(contract);
        })
        .then((schema) => {
            return Promise.resolve(schema);
        })
        .catch((err) => Promise.reject(err));
};

const generateSchemas = (paths) => {
    const buildOps = paths.map((path) => generateSingleSchema(path));
    return new Promise((resolve, reject) => {
        // wait for al lthe build operations to complete
        Promise.all(buildOps)
            .then((schemas) => {
                // combine any duplicated endpoints
                const combined = schemas.reduce((output, schema) => {
                    const updated = Object.assign({}, output);
                    Object.keys(schema).forEach((endpoint) => {
                        if (updated[endpoint]) {
                            // concat the schema arrays
                            updated[endpoint] = updated[endpoint].concat(schema[endpoint]);
                        }
                        else {
                            updated[endpoint] = schema[endpoint];
                        }
                    });
                    return updated;
                }, {});

                resolve(combined);
            })
            .catch((err) => {
                reject(err);
            });
    });
};

fetchContracts()
    .then((contracts) => {
        return generateSchemas(contracts);
    })
    .then((schemas) => {
        // write the schemas
        console.log('Generating JSON Schemas for:');
        console.log(Object.keys(schemas).join('\n'));
        return writeSchemaFile(schemas);
    })
    .then(() => {
        console.log('Completed successfully');
        process.exit(0);
    })
    .catch((err) => {
        console.log(err);
        process.exit(1);
    });