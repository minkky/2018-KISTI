var value = require('./value');

function create2dSphereIndex(collectionName) {
    return new Promise(resolve => {
        value.connect()
            .then(function (db) {
                db.collection(collectionName).createIndex({location: "2dsphere", time: 1}, function (err, result) {
                    resolve(collectionName + ":" + result);
                });
            });
    });
}

function createLocationTimeIndex(collectionName){
    return new Promise(resolve => {
        value.connect()
            .then(function (db) {
                db.collection(collectionName).createIndex({ "location.0" : 1, "location.1" : 1, time: 1}, function (err, result) {
                    resolve(collectionName + ":" + result);
                });
            });
    });
}

exports.create2dSphereIndex = create2dSphereIndex;
exports.createLocationTimeIndex = createLocationTimeIndex;