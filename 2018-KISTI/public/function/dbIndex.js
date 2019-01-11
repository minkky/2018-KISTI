let create2dSphereIndex = function (db, collectionName, callback) {
    let collection = db.collection(collectionName);
    collection.ensureIndex(
        {location: "2dsphere", time: 1}, function (err, result) {
            callback(result);
        });
};

let create2dIndex = function (db, collectionName, callback) {
    let collection = db.collection(collectionName);
    collection.ensureIndex(
        {location: "2d"}, function (err, result) {
            callback(result);
        });
};

let create2dAndTimeIndex = function (db, collectionName, callback) {
    let collection = db.collection(collectionName);
    collection.ensureIndex(
        {location: "2d", time: 1}, function (err, result) {
            callback(result);
        });
};

let createXYZCompoundUniqueIndex = function (db, collectionName, callback) {
    db.collection(collectionName).ensureIndex({
        x_index: 1,
        y_index: 1,
        t_index: 1
    }, {unique: true}, function (err, result) {
        console.log("> created XYZ Compound Unique Index");
        callback(result);
    });
};

let createThingIdsIndex = function (db, collectionName, callback) {
    let collection = db.collection(collectionName);
    collection.ensureIndex(
        {thingIds: 1}, function (err, result) {
            callback(result);
        });
};

let createNThingIdsIndex = function (db, collectionName, callback) {
    let collection = db.collection(collectionName);
    collection.ensureIndex(
        {NThingIds: 1}, function (err, result) {
            callback(result);
        });
}

let createThingNthingIndex = function (db, collectionName, callback) {
    let collection = db.collection(collectionName);
    collection.ensureIndex(
        {thingIds: 1, NThingIds: 1}, function (err, result) {
            callback(result);
        });
}

exports.createThingNthingIndex = createThingNthingIndex;
exports.createNThingIdsIndex = createNThingIdsIndex;
exports.create2dAndTimeIndex = create2dAndTimeIndex;
exports.create2dIndex = create2dIndex;
exports.createThingIdsIndex = createThingIdsIndex;
exports.createXYZCompoundUniqueIndex = createXYZCompoundUniqueIndex;
exports.create2dSphereIndex = create2dSphereIndex;