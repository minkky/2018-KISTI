let MongoClient = require('mongodb').MongoClient;
let url = 'mongodb://localhost:27017/KISTI';
let db = null;

function connect() {
    return new Promise(resolve => {
        MongoClient.connect(url, function (err, database) {
            if (err) throw err;
            db = database;
            resolve(database);
        });
    });
}

function end() {
    db.close();
}

function getDB() {
    return db;
}

exports.getDB = getDB;
exports.connect = connect;
exports.end = end;