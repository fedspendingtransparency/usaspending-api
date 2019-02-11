const path = require('path');
const fse = require('fs-extra');
const glob = require('glob');
const aglio = require('aglio');

const { fetchContracts, readContract } = require('../common/common.js');

const writeDocs = (output, filePath) => new Promise((resolve, reject) => {
    // change the file name
    const fileName = path.basename(filePath, '.md') + '.html';
    const destPath = path.resolve(process.cwd(), './docs', path.dirname(filePath), fileName);
    fse.outputFile(destPath, output, 'utf8', (err) => {
        if (err) {
            return reject(err);
        }
        resolve(destPath);
    })
});

const buildDocFromContract = (contract) => new Promise((resolve, reject) => {
    aglio.render(contract, {
        theme: 'flatly'
    }, (err, html) => {
        if (err) {
            return reject(err);
        }

        resolve(html);
    });
});

const generateSingleDoc = (contractPath) => {
    return readContract(contractPath)
        .then((contract) => {
            return buildDocFromContract(contract);
        })
        .then((html) => {
            return writeDocs(html, path.relative('./contracts', contractPath));
        })
        .catch((err) => Promise.reject(err));
};

const generateDocs = (paths) => {
    const buildOps = paths.map((path) => generateSingleDoc(path));
    return Promise.all(buildOps);
};

fetchContracts()
    .then((contracts) => {
        return generateDocs(contracts);
    })
    .then((docPaths) => {
        console.log('Generated docs:');
        docPaths.forEach((docPath) => {
            console.log(docPath);
        });
        process.exit(0);
    })
    .catch((err) => {
        console.log(err);
        process.exit(1);
    });