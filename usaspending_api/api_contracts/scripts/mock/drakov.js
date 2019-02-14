const drakov = require('drakov');

const drakovOptions  = {
    sourceFiles: './contracts/**/*.md',
    serverPort: 5000,
    autoOptions: true
};


drakov.run(drakovOptions, function(err){
    if (err) {
        throw err;
        console.log('-- STARTED --');
    }
});
