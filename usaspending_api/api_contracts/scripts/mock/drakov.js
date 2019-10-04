const drakov = require('drakov');

const drakovOptions  = {
    sourceFiles: './contracts/**/*.md',
    serverPort: 5000,
    autoOptions: true,
    delay: 250 // 0.25 seconds
};


drakov.run(drakovOptions, function(err){
    if (err) {
        throw err;
        console.log('-- STARTED --');
    }
});
