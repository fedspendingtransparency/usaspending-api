const drakov = require('drakov');

const drakovOptions  = {
    sourceFiles: './contracts/**/*.md',
    serverPort: 5000,
    autoOptions: true,
    delay: 500 // 1/2 a second
};


drakov.run(drakovOptions, function(err){
    if (err) {
        throw err;
        console.log('-- STARTED --');
    }
});
