const Bottleneck = require("bottleneck"),
	nodemailer = require('nodemailer'),
	logger = require('./utils/logger'),
	fs = require('fs-extra'),
	csv = require('csv-parser'),
	fastcsv = require('fast-csv'),
	axios = require('axios'),
	moment = require('moment');

let csvName, dbName;

if(process.env.GOLIVE == 1) {
	csvName = "./data/prod.csv"
	dbName = './db/db-prod.json';

} else {
	csvName = "./data/stage.csv";
	dbName = './db/db-stage.json';
}

// Database stuff
const low = require('lowdb');
const FileSync = require('lowdb/adapters/FileSync');
const adapter = new FileSync(dbName);
const db = low(adapter);
const shortid = require('shortid');

// Set some defaults
db.defaults({ sentEmails: [], failedEmails: [] })
  .write();

//Config File
let config = require('./config/config.json');

// Creates an object of bottleneck with the required settings - Infinite requests, one request ever 1.2 seconds, ??, ??, ??
let limiter = new Bottleneck(0, 100, -1, Bottleneck.strategy.BLOCK, true);
let outputArray = [];

let today = moment().format("DD-MM-YYYY");


readData(csvName).then((data) => {

	let uniqueId = shortid.generate();

	for (let userData of data) {

		checkUserInDb(userData.user).then((res) => {

			return prepareMail(userData, config.sender, config.subject, config.options);

		}).then((res) => {

			// Bottleneck to limit the sending
			return limiter.schedule(send, res.transporter, res.mailOptions);

		}).then((res) => {

			logger.verbose(res);
			outputArray.push(["Sent", res.successUser])

			// Log to DB
			db.get('sentEmails')
			  .push({ id: uniqueId, user: res.successUser, date: today, messageID: res.info.messageId})
			  .write()

		}).catch((err) => {
			
			if(err.hasOwnProperty("query")) { // Handle failed querys
				logger.verbose('User already exists in DB')

			} else if (err.hasOwnProperty("failedUser")) { // Handle failed sending
				logger.error(err);

				outputArray.push(["Failed", err.failedUser, err.reason]);

				db.get('failedEmails')
				  .push({ id: uniqueId, user: err.failedUser, date: today, reason: err.reason})
				  .write()

			} else {
				logger.error(err);
			}
		});
	}
}).catch((err) => {
    logger.error(err);
})

function readData(file) {
	return new Promise((resolve, reject) => {

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

function checkUserInDb(userId) {
	return new Promise((resolve, reject) => {

		const query = db.get('sentEmails')
		  .find({ user: `${ userId }@cisco.com` })
		  .value()

		 if(query == undefined) {
		 	resolve(query);
		 } else {
		 	reject({query: false})
		 }

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
