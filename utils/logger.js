
var winston = require('winston');
var fs = require('fs');
const logDir = './logs';

// Create time stamp
const tsFormat = () => (new Date()).toLocaleTimeString();

// Create the log directory if it does not exist
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}


var winston = new (winston.Logger)({  
    transports: [
        new (winston.transports.Console)({ timestamp: tsFormat, colorize: true, level: 'debug' }),
        new (winston.transports.File)({ filename: __dirname + '/../logs/production.log', timestamp: tsFormat, level: 'debug' })
    ]
});

winston.info('Chill Winston, the logs are being captured 2 ways - console and file')

module.exports = winston;  

// Loggly - I dont need to use this yet, it's for cloud logging
/*
var Loggly = require('winston-loggly').Loggly;
var loggly_options={ subdomain: "mysubdomain", inputToken: "efake000-000d-000e-a000-xfakee000a00" }
logger.add(Loggly, loggly_options);
*/