const Bottleneck = require("bottleneck");
const nodemailer = require('nodemailer');
const logger = require('./utils/logger');
//const fs = require('fs')
const fs = require('fs-extra');
const csv = require('csv-parser');
const fastcsv = require('fast-csv');
const axios = require('axios');

//Config File
var config = require('./config/config.json');

// Creates an object of bottleneck with the required settings - Infinite requests, one request ever 1.2 seconds, ??, ??, ??
var limiter = new Bottleneck(0, 100, -1, Bottleneck.strategy.BLOCK, true);
var outputArray = [];

if(process.env.GOLIVE == 1) {

	readData('data/test.csv').then((data) => {

		for (let userData of data) {

			prepareMail(userData, config.sender, config.subject, config.options).then((res) => {

				// Bottleneck to limit the sending
				return limiter.schedule(send, res.transporter, res.mailOptions);

			}).then((res) => {

				logger.verbose(res);
				outputArray.push(["Sent", res.successUser])

			}).catch((err) => {
				
				logger.error(err);

				if(err.hasOwnProperty("failedUser")) {
					outputArray.push(["Failed", err.failedUser, err.reason]);
				}
			});
		}
	}).catch((err) => {
        logger.error(err);
    })

} else {

	prepareMail(config.testUser, config.sender, config.subject, config.options).then((res) => {

		// Bottleneck to limit the sending
		return limiter.schedule(send, res.transporter, res.mailOptions)
				
	}).then((res) => {
		logger.verbose(res);

	}).catch((err) => {
		logger.error(err);
	}); 
}


function readData(file) {
	return new Promise((resolve, reject) => {

		let file = './data/test.csv';
		let allData = [];
		fs.createReadStream(file)
		  .pipe(csv())
		  .on('data', (data) => {
		  	allData.push(data)
		  })
		  .on('end', () => {
		  	resolve(allData);
		  }) 
		  .on('error', () => {
		  	reject("got damn");
		  })
	})
}


function prepareMail(userData, sender, subject, options) {
	return new Promise((resolve, reject) => {

		// For test user
		let user = userData.user || userData;
		let days = userData.days || 0;

		let transporter = nodemailer.createTransport({
			host: 'outbound.cisco.com',
			port: 25,
			secure: false
		});

		let p1 = fs.readFile('config/template.txt', 'utf8');
		let p2 = axios.get(`http://directory.cisco.com/dir/details-json/${ user }`);

		Promise.all([p1, p2]).then((res) => {

		    // Replace varibles in the template
		    template = res[0].replace('{username}', res[1].data.prfn);
		    template = template.replace('{elapsedDays}', days);

		    let mailOptions = {
				from: sender, // Sender
				to: `${ user }@cisco.com`,
//				cc: options.includeManager ? res[1].data.mgrid : "",
				subject: subject,
				html: template
			};

			return {transporter, mailOptions};
		 
		}).then((res) => {
			resolve({transporter: res.transporter, mailOptions: res.mailOptions});

		}).catch((err) => {
			logger.error(err);
		});

	})
}

function send(transporter, mailOptions) {
	return new Promise((resolve, reject) => {

		transporter.sendMail(mailOptions, (error, info) => {
			if (error) {

				let errorReject = error.rejected == undefined ? 'undefined' : error.rejected[0];

				reject({'failedUser': errorReject, 'reason': error.response || error});
				return;
			}

			resolve({ 'successUser': mailOptions.to, 'messageID': info.messageId, 'info': info.response  });
		});

	})
}

limiter.on('idle', function () {
		// This will be called when the nbQueued() drops to 0 AND there is nothing currently running in the limiter. 
		logger.silly("Bottleneck idle");
		
		if(outputArray.length > 0) {
			let currentDT = new Date().toISOString().replace('T', ' ').replace(/\..*$/, '').replace(/:/g,'.');
			fastcsv.writeToStream(fs.createWriteStream(`output/${ currentDT }.csv`), outputArray, {headers: false});
		}

})
