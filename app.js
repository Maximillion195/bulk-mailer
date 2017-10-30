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
	logger.verbose("Production running");
} else {
	csvName = "./data/stage.csv";
	dbName = './db/db-stage.json';
	logger.verbose("Stage running");
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
let today = moment().format("DD-MM-YYYY");


readData(csvName).then((data) => {

	let uniqueId = shortid.generate();

	for (let userData of data) {

		// Check for any blank cells in data
		if(userData.user == "") {
			break;
		}

		checkUserExists(userData.user).then((res) => {


			// Add the extra data to userData
			userData.prfName = res.prfn;
			//userData.manager = res.mgrid;

			return checkUserInDb(userData.user);

		}).then((res) => {
			return prepareMail(userData, config.sender, config.subject, config.options);

		}).then((res) => {
			// Bottleneck to limit the sending
			return limiter.schedule(send, res.transporter, res.mailOptions);
		}).then((res) => {

			logger.verbose(res);

			// Log to DB
			db.get('sentEmails')
			  .push({ id: uniqueId, user: res.successUser, date: today, messageID: res.info.messageId})
			  .write()

		}).catch((err) => {
			
			if(err.hasOwnProperty("query")) { // Handle failed querys
				logger.verbose(`User ${ err.user } already mailed recently`);

			} else if (err.hasOwnProperty("failedUser")) { // Handle failed sending
				logger.verbose(`User ${ err.failedUser } failed. ${ err.reason }`);

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
		  	reject("Failed reading the CSV");
		  })
	})
}

function checkUserExists(user) {
	return new Promise((resolve, reject) => { 

		axios.get(`http://directory.cisco.com/dir/details-json/${ user }`).then((res) => {

			if (res.data.hasOwnProperty('uid')) {
				resolve(res.data);
			} else {
				reject({ failedUser: user, reason: 'User does not exist' });
			}
		}).catch((err) => {
			logger.error(err);
		});
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
		 	reject({query: false, user: userId})
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

		fs.readFile('config/template.txt', 'utf8').then((res) => {

		    // Replace variables in the template
		    template = res.replace('{username}', userData.prfName);
		    template = template.replace('{elapsedDays}', days);

		    let mailOptions = {
				from: sender, // Sender
				to: `${ user }@cisco.com`,
				cc: options.includeManager ? `${ userData.manager }@cisco.com` : "",
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

			if (mailOptions.hasOwnProperty('cc')) {
				resolve({ 'successUser': mailOptions.to, 'ccUser': mailOptions.cc , 'messageID': info.messageId, 'info': info.response  });

			} else {
				resolve({ 'successUser': mailOptions.to, 'messageID': info.messageId, 'info': info.response  });
			}

		});

	})
}