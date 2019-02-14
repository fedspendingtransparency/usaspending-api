const fs = require('fs');
const glob = require('glob');

const fetchContracts = () => new Promise((resolve, reject) => {
    glob('./contracts/**/*.md', {}, (err, files) => {
        if (err) {
            return reject(err);
        }

        resolve(files);
    });
});

const readContract = (path) => new Promise((resolve, reject) => {
    fs.readFile(path, {
        encoding: 'utf8'
    }, (err, data) => {
        if (err) {
            return reject(err);
        }
        resolve(data);
    });
});

module.exports = {
    fetchContracts,
    readContract
};