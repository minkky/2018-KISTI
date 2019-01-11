let config = require('../../config');
let MongoClient = require('mongodb').MongoClient;
let url = config.mongodb_url + config.DB_USER;
let collections = undefined;
let minLNG = 190;
let minLAT = 100;
let minTIME;
let x_size = 10000;
let y_size = 10000;
let t_size = "100m";
let db = null;
let queries = [];
let search_loc_interval;
let search_time_interval;
let smax = 0.0001;
let tmax = 60;
let joinCollections = ["IK1001", "IK1008"];
let partitionSettingDone = false;

if (db == null) {
    MongoClient.connect(url, function (err, database) {
        if (err) {
            console.error('MongoDB 연결 실패', err);
            return;
        }
        db = database;
    });
}

function setSMax(s) {
    smax = s;
}

function setTMax(t) {
    t = t.replace('s', '');
    tmax = t;
}

function getSMax() {
    return smax;
}

function getTMax() {
    return tmax;
}

function setSearchLocInterval(loc_diff) {
    search_loc_interval = loc_diff;
}

function setSearchTimeInterval(time_diff) {
    search_time_interval = time_diff;
}

function getSearchLocInterval() {
    return search_loc_interval;
}

function getSearchTimeInterval() {
    return search_time_interval;
}

function end() {
    db.close();
}

function setQueries(query) {
    queries = query;
}

function getQueries() {
    return queries;
}

function setCollections(names) {
    collections = names;
}

function setMinLNG(lng) {
    minLNG = lng;
}

function setMinLAT(lat) {
    minLAT = lat;
}

function setMinTIME(time) {
    minTIME = time;
}

function setXSize(size) {
    x_size = size;
}

function setYSize(size) {
    y_size = size;
}

function setTSize(size) {
    t_size = size;
}

function getCollectionList() {
    return new Promise(resolve => {
        let cnt = 0;
        collections = [];
        db.listCollections().toArray(function (err, result) {
            result.forEach(function (doc) {
                if (doc.name.indexOf(config.partition) == -1 && doc.name.indexOf('system') == -1) {
                    collections.push(doc.name);
                }
                else {
                    cnt++;
                }
            });
            if (result.length == collections.length + cnt) {
                setCollections(collections.sort());
                resolve(collections);
            }
        });
    });
}

function getCollections() {
    if (collections == undefined)
        getCollectionList()
            .then(function (collection) {
                return collection;
            });
    else
        return collections;
}

function getMinLNG() {
    return minLNG;
}

function getMinLAT() {
    return minLAT;
}

function getMinTIME() {
    return minTIME;
}

function getXSize() {
    return x_size;
}

function getYSize() {
    return y_size;
}

function getTSize() {
    return t_size;
}

function getDB() {
    return db;
}

function transformSec(time) {
    return new Promise(resolve => {
        if (time.indexOf("h") != -1) {
            time = time.substring(0, time.indexOf("h"));
            time = time * 60 * 60;
            resolve(parseFloat(time));
        }
        else if (time.indexOf("m") != -1) {
            time = time.substring(0, time.indexOf("m"));
            time = time * 60;
            resolve(parseFloat(time));
        }
        else if (time.indexOf("s") != -1) {
            time = time.substring(0, time.indexOf("s"));
            resolve(parseFloat(time));
        }
    });
}

function setJoinCollection(collection) {
    joinCollections = collection;
}

function getJoinCollection() {
    return joinCollections;
}

function setPartitionSettingDone(status) {
    partitionSettingDone = status;
}

function getPartitionSettingDone() {
    return partitionSettingDone;
}

exports.transformSec = transformSec;
exports.setCollections = setCollections;
exports.setMinLNG = setMinLNG;
exports.setMinLAT = setMinLAT;
exports.setMinTIME = setMinTIME;
exports.setXSize = setXSize;
exports.setYSize = setYSize;
exports.setTSize = setTSize;
exports.getCollectionList = getCollectionList;
exports.getCollections = getCollections;
exports.getMinLNG = getMinLNG;
exports.getMinLAT = getMinLAT;
exports.getMinTIME = getMinTIME;
exports.getXSize = getXSize;
exports.getYSize = getYSize;
exports.getTSize = getTSize;
exports.getDB = getDB;
exports.getQueries = getQueries;
exports.setQueries = setQueries;
exports.end = end;
exports.setSearchLocInterval = setSearchLocInterval;
exports.getSearchLocInterval = getSearchLocInterval;
exports.setSearchTimeInterval = setSearchTimeInterval;
exports.getSearchTimeInterval = getSearchTimeInterval;
exports.setSMax = setSMax;
exports.setTMax = setTMax;
exports.getSMax = getSMax;
exports.getTMax = getTMax;
exports.setJoinCollection = setJoinCollection;
exports.getJoinCollection = getJoinCollection;
exports.setPartitionSettingDone = setPartitionSettingDone;
exports.getPartitionSettingDone = getPartitionSettingDone;
