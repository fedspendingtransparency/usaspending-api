const drakov = require('drakov');

const drakovOptions  = {
    sourceFiles: './contracts/**/*.md',
    serverPort: 5000,
    autoOptions: true,
    delay: 2500 // 2.5 seconds
};


drakov.run(drakovOptions, function(err){
    if (err) {
        throw err;
        console.log('-- STARTED --');
    }
});
