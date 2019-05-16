const drakov = require('drakov');

const drakovOptions  = {
    sourceFiles: './contracts/**/*.md',
    serverPort: 5000,
    autoOptions: true,
    delay: 10000 // 10 seconds
};


drakov.run(drakovOptions, function(err){
    if (err) {
        throw err;
        console.log('-- STARTED --');
    }
});
