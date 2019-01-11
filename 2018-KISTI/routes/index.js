var express = require('express');
var dbIndex = require('../public/function/dbIndex');
var value = require('../public/function/value');
var router = express.Router();
var partition = "Partition";
var async = require('async');
let copyCollectionDone = false;
let config = require('../config');
let MongoClient = require('mongodb').MongoClient;
let url = config.mongodb_url + config.DB_USER;
let db;

MongoClient.connect(url, function (err, database) {
    if (err) {
        console.error('MongoDB 연결 실패', err);
        return;
    }
    db = database;
});

/* GET home page. */
router.get('/', function (req, res, next) {
    res.render('index', {title: 'Spatio-Temporal Join Prototype - KISTI'});
});

router.get('/setting', function (req, res, next) {
    res.render('setting', {title: 'Partition Index Building'});
});

function collectionCopyAndDrop() {
    return new Promise(resolve => {
        let collection = value.getCollections();
        let cnt = 0;

        if (db == null)
            db = value.getDB();

        collection.forEach(element => {
            async.waterfall([
                function (callback) {
                    let cols = element.split("_");
                    callback(null, cols[0], cols.length);
                },
                function (cols, colsLength, callback) {
                    if (colsLength != 1) {
                        db.collection(element).find().toArray(function (err, result) {
                            if (err) ;
                            callback(null, cols, result);
                        });
                    }
                    else
                        callback(null, cols, "error");
                },
                function (cols, result, callback) {
                    if (result == "error")
                        callback(null, cols, false);
                    db.collection(cols).insertMany(result, {safe: true}, function (err, doc) {
                        if (err) ;
                        if (doc) callback(null, cols, true);
                    });
                },
                function (col, dropOk, callback) {
                    if (dropOk)
                        db.collection(element).drop(function (err, drop) {
                            if (err) ;
                            if (drop) console.log(element, "to", col);
                        });
                    callback(null, cnt + 1);
                },
                function (count, callback) {
                    cnt = count;
                    callback(null, cnt);
                }
            ], function (err, result) {
                if (result == collection.length) {
                    copyCollectionDone = true;
                    resolve(true);
                }
            });
        });
    });
}

function createIndexToAll() {
    return new Promise(resolve => {
        let collections = value.getCollections();
        let cnt = 0;
        collections.forEach(collection => {
            dbIndex.create2dAndTimeIndex(value.getDB(), collection.split("_")[0], function (result) {
                if (result) cnt++;
                if (cnt == collections.length)
                    resolve("index created all");
            });
        });
    });
}

router.get('/appendPartition', function (req, res, next) {
    collectionCopyAndDrop()
        .then(function (re) {
            console.log("all done");
            setTimeout(
                function () {
                    createIndexToAll()
                        .then(function (result) {
                            console.log(result);
                        })
                }, 8000);
            // test timeout 작동되는지,
            /*createIndexToAll()
                .then(function (result) {
                    console.log(result);
                });*/
        });
    res.redirect('/setting');
});

function getPartitionInfo() {
    return new Promise(function (resolve, reject) {
        let interval_info = [];
        value.getDB().listCollections({name: partition}).toArray(function (err, result) {
            if (result.length != 0) {
                value.getDB().collection(partition).find({_id: 'metadata'}).forEach(function (res) {
                    if (res) {
                        interval_info.push(res.x_size);
                        interval_info.push(res.t_size);
                        resolve(interval_info);
                    }
                });
            }
            else {
                reject("undefined");
            }
        });
    });
}

router.get('/search', function (req, res, next) {
    let col = value.getCollections();
    value.getCollectionList()
        .then(function (result) {
            console.log(result);
            col = result;
        });

    getPartitionInfo()
        .then(function (result) {
            res.render('search', {
                title: 'Spatio-Temporal Join - Search',
                distance_interval: result[0],
                time_interval: result[1],
                tmax: value.getTMax(),
                smax: value.getSMax(),
                collections: col,
                len: col.length
            });
        })
        .catch(function (err) {
            let collection = value.getCollections();
            res.render('search', {
                title: 'Spatial Temporal JOIN - Search',
                distance_interval: undefined,
                time_interval: undefined,
                tmax: value.getTMax(),
                smax: value.getSMax(),
                collections: collection,
                len: 0
            });
        });
});

module.exports = router;
