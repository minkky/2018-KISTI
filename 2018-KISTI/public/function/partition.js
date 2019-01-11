let value = require('./value');
let dbIndex = require('./dbIndex');
let moment = require('moment');
let config = require('../../config');
let partition = config.partition;
let MongoClient = require('mongodb').MongoClient;
let url = config.mongodb_url + config.DB_USER;
let db;
let latestUpdate = "";
let partitionInfoUpdated = false;

MongoClient.connect(url, function (err, database) {
    if (err) {
        console.error('MongoDB 연결 실패', err);
        return;
    }
    db = database;
});

let createAndFillPartition = function () {
    return new Promise(resolve => {
        db.collection(partition).find({_id: "metadata"}).forEach(function (doc) {
            latestUpdate = doc.latestUpdate;
            // 만약 이전 날짜가 들어오면 update를 해줄지 여부를 결정해야함.
            let metadata = {
                x_min: value.getMinLNG(),
                y_min: value.getMinLAT(),
                t_min: value.getMinTIME(),
                x_size: value.getXSize(),
                y_size: value.getYSize(),
                t_size: value.getTSize(),
                latestUpdate: latestUpdate
            };
            console.log(metadata);
            try {
                createPartition(metadata)
                    .then(function (result) {
                        return new Promise(resolve1 => {
                            resolve1("created Index");
                        })
                            .then(function (result) {
                                return new Promise(resolve2 => {
                                    if (result) {
                                        let collections;
                                        value.getCollectionList()
                                            .then(function (result) {
                                                collections = result;
                                                let i;
                                                for (i = 0; i < collections.length; i++) {
                                                    dbIndex.create2dAndTimeIndex(db, collections[i], function () {
                                                        ;
                                                    });
                                                }
                                                if (i == collections.length) {
                                                    resolve2("> create 2d and Time index to all collections");
                                                }
                                            });
                                    }
                                })
                                    .then(function (result) {
                                        console.log(result);
                                        try {
                                            fillPartition()
                                                .then(function (res) {
                                                    console.log(res);
                                                    resolve(res);
                                                })
                                                .catch(function (err) {
                                                    console.log(err);
                                                });
                                        }
                                        catch (e) {
                                            ;
                                        }
                                    });
                            });
                    });
            }
            catch (e) {
                ;
            }
        });
    });
};

function dropExistPartition() {
    return Promise(resolve => {
        db.collection(partition).drop(function (err, delOK) {
            if (err) throw err;
            if (delOK) {
                console.log("> exist Partition Deleted.");
                resolve(db);
            }
        });
    })
}


let createPartition = function (metadata) {
    return new Promise(function (resolve) {
            if (db == null) {
                MongoClient.connect(url, function (err, database) {
                    if (err) {
                        console.error('MongoDB 연결 실패', err);
                        return;
                    }
                    db = database;
                });
            }
            else {
                resolve(db);
            }
        }
    ).then(function (database) {
        return new Promise(resolve1 => {
            db.collection(partition).update({_id: "metadata"}, {$set: metadata}, {upsert: true}, function (err, result) {
                if (err) throw err;
                if (result) {
                    if (result.result.nModified == 0)
                        partitionInfoUpdated = false;
                    else
                        partitionInfoUpdated = true;
                    console.log("> Partition collection created and metadata setting");
                    resolve1(partitionInfoUpdated);
                }
            });
        })
            .then(function (updated) {
                return new Promise(resolve => {
                    if (updated)
                        db.collection(partition).drop(function (err, del) {
                            if (err) ;
                            if (del) {
                                console.log(partition, "drop");
                                resolve(updated);
                            }
                        });
                    else resolve(updated);
                })
                    .then(function (updated) {
                        return new Promise(resolve => {
                            if (updated)
                                db.collection(partition).update({_id: "metadata"}, {$set: metadata}, {upsert: true}, function (err, result) {
                                    if (err) ;
                                    resolve(true);
                                });
                            else resolve(true);
                        })
                            .then(function (result) {
                                return new Promise(resolve1 => {
                                    console.log("metadata updated :", partitionInfoUpdated);
                                    dbIndex.createXYZCompoundUniqueIndex(db, partition, function (result) {
                                        resolve1(result);
                                    });
                                })
                                    .then(function (result) {
                                        return new Promise(resolve1 => {
                                            dbIndex.createThingIdsIndex(db, partition, function (result) {
                                                resolve1(result);
                                            });
                                        })
                                            .then(function (result) {
                                                return new Promise(resolve1 => {
                                                    dbIndex.createNThingIdsIndex(db, partition, function (result) {
                                                        resolve1(result);
                                                    });
                                                })
                                                    .then(function (result) {
                                                        return new Promise(resolve => {
                                                            dbIndex.createThingNthingIndex(db, partition, function (result) {
                                                                resolve(result);
                                                            });
                                                        })
                                                    });
                                            });
                                    });
                            });
                    });
            });
    });
};

function existPartition() {
    return new Promise(resolve => {
        let arr = [];
        if (db == null) {
            MongoClient.connect(url, function (err, database) {
                if (err) {
                    console.error('MongoDB 연결 실패', err);
                    return;
                }
                db = database;
            });
        }
        db.listCollections().toArray(function (err, result) {
            result.forEach(function (doc) {
                arr.push(doc.name);
            });
            if (result.length == arr.length) {
                resolve(arr.includes(partition));
            }
        });
    });
}

function getQuery(index) {
    return {x_index: index[0], y_index: index[1], t_index: index[2]};
}

function getQueryWithThingId(index, id) {
    return {
        $set: {x_index: index[0], y_index: index[1], t_index: index[2]},
        $addToSet: {thingIds: {$each: [id]}}
    };
}

function getQueryWithNThingId(index, id) {
    return {
        $set: {x_index: index[0], y_index: index[1], t_index: index[2]},
        $addToSet: {NThingIds: {$each: [id]}}
    };
}

function getTimeInterval(time) {
    let minTime = new Date(value.getMinTIME());
    minTime.setHours(minTime.getHours() + 9);
    return Math.abs(new Date(time) - minTime);
}

function getTIndex(time) {
    let t = getTimeInterval(time);
    return Math.ceil(t / (value.getTSize() * 1000));
}

function getXBounds(value, size) {
    let arr = [];
    let left = value + (size * -1);
    arr.push(getXIndex(left));
    let right = value + (size * 1);
    arr.push(getXIndex(right));
    return arr;
}

function getYBounds(value, size) {
    let arr = [];
    let left = value + (size * -1);
    arr.push(getYIndex(left));
    let right = value + (size * 1);
    arr.push(getYIndex(right));
    return arr;
}

function getTBound(value, size) {
    let arr = [];
    let left = moment(value).add(size * -1, "seconds");
    arr.push(getTIndex(left));
    let right = moment(value).add(size, "seconds");
    arr.push(getTIndex(right));
    return arr;
}

function getNearIds(x_index, y_index, t_index, xl, xr, yl, yr, tl, tr) {
    return new Promise(resolve => {
        let nearID = new Set();

        for (var i = xl; i <= xr; i++) {
            for (var j = yl; j <= yr; j++) {
                for (var k = tl; k <= tr; k++) {
                    if (!(x_index == i && y_index == j && t_index == k) && (i >= 1 && j >= 1 && k >= 1)) {
                        nearID.add([i, j, k]);
                    }
                    if (i == xr && j == yr && k == tr)
                        resolve(nearID);
                }
            }
        }
    });
}

let getTotalCount = function (collections) {
    return new Promise(resolve => {
        let total = 0;
        let cnt = 0;
        collections.forEach(function (col, idx, err) {
            let cursor = db.collection(col).find();
            cursor.toArray(function (err, result) {
                cnt++;
                total += result.length;
                if (cnt == collections.length)
                    resolve(total);
            });
        });
    })
};

let getFiveCollections = function (collections) {
    return new Promise(resolve => {
        if (collections.length > 5) {
            //value.setCollections(collections.slice(0, 5));
            resolve(collections.slice(0, 5));
        }
        else
            resolve(collections);
    });
}

let compareDate = function (src, target) {
    return moment(src).diff(moment(target), 'days');
}

let getCollectionsAfterLatestUpdate = function () {
    return new Promise(resolve => {
        let collection = value.getCollections();
        let cols = [];
        let comps = [];
        collection.forEach(element => {
            function f() {
                return new Promise(resolve1 => {
                    if (element.split("_").length != 1) {
                        if (compareDate(latestUpdate, element.split("_")[1]) >= 0) {
                            cols.push(element);
                        }
                    }
                    else if (partitionInfoUpdated && !(element.includes(partition) && element.includes("sys"))) {
                        cols.push(element);
                    }
                    resolve1(cols);
                });
            }

            f()
                .then(function (res) {
                    comps.push(element);
                    if (comps.length == collection.length) {
                        comps = null;
                        resolve(cols);
                    }
                });
        });
    });
}

let fillPartition = function () {
    return new Promise(resolve => {
        let collections = value.getCollections();
        /*getFiveCollections(collections)
            .then(function (res) {
                console.log(res);
            });*/
        /* collection 5 개만 가져올 수 있게 하기 */

        getCollectionsAfterLatestUpdate(collections)
            .then(function (result) {
                collections = result;
                if (collections.length == 0)
                    resolve("> Filled Partition Collection");
                console.log(collections);
                let cnt = 0;
                let createtime = process.hrtime();
                getTotalCount(collections)
                    .then(function (total) {
                        console.log(total);
                        collections.forEach(
                            function (collection, idx, err) { // 아래 부분 forEach로 변경하여 구현할 것!
                                let cursor = db.collection(collection).find();
                                cursor.forEach(function (doc) {
                                    let thingId = doc.thingId;
                                    if (doc.location[0] < value.getMinLNG() || doc.location[1] < value.getMinLAT() || value.getSearchTimeInterval(doc.time) < 0) {
                                        ;
                                    }
                                    else {
                                        let x_index = getXIndex(doc.location[0]);
                                        let y_index = getYIndex(doc.location[1]);
                                        let time = getTime(doc.time);
                                        let t_index = getTIndex(time);
                                        let thing = [x_index, y_index, t_index];
                                        let query = getQuery(thing);
                                        let data = getQueryWithThingId(thing, thingId);
                                        let upsert = {upsert: true, safe: true};
                                        let xls = getXBounds(doc.location[0], value.getSMax());
                                        let yls = getYBounds(doc.location[1], value.getSMax());
                                        let tls = getTBound(time, value.getTMax());
                                        let xl = xls[0];
                                        let xr = xls[1];
                                        let yl = yls[0];
                                        let yr = yls[1];
                                        let tl = tls[0];
                                        let tr = tls[1];

                                        db.collection(partition).updateOne(query, data, upsert, function (err) {
                                            return new Promise((resolve1, reject1) => {
                                                if (err) ;
                                                getNearIds(x_index, y_index, t_index, xl, xr, yl, yr, tl, tr)
                                                    .then(function (id) {
                                                        let nearID = Array.from(id);
                                                        if (nearID.length != 0) {
                                                            nearID.forEach(function (nId) {
                                                                db.collection(partition).updateOne(getQuery(nId), getQueryWithNThingId(nId, thingId), upsert)
                                                                    .then(function (err, res) {
                                                                        if (err) ;
                                                                        //console.log(process.hrtime(createtime));
                                                                    })
                                                                    .catch(function (err) {
                                                                        if (err.code == 11000) ;
                                                                    })
                                                                    .then(function (res) {
                                                                        ;
                                                                    });
                                                            });
                                                        }
                                                        resolve1("one line done");
                                                    });
                                            })
                                                .catch(function (err) {
                                                    if (err.code == 11000) ;
                                                })
                                                .then(function (res) {
                                                    ;
                                                });
                                        });
                                    }
                                    cnt++;
                                    if (total == cnt) {
                                        console.log(total, cnt);
                                        resolve("> Filled Partition Collection");
                                    }
                                });
                            }
                        );
                    });
            });
    });
};

let getXIndex = function (lng) {
    return Math.ceil((lng - value.getMinLNG()) / value.getXSize());
};

let getYIndex = function (lat) {
    return Math.ceil((lat - value.getMinLAT()) / value.getYSize());
};

function getTime(time) {
    let front = time.split('T')[0];
    let end = time.split('T')[1];
    let year = parseInt(front.split('-')[0]);
    let month = parseInt(front.split('-')[1]);
    let day = parseInt(front.split('-')[2]);
    let hour = parseInt(end.split(':')[0]);
    let min = parseInt(end.split(':')[1]);
    let ss = parseInt(end.split(':')[2]);
    let date = new Date(Date.UTC(year, month - 1, day, hour, min, ss));
    return date;
}

function updatePartitionThingIdsWithSort() {
    return new Promise(resolve5 => {
        var cnt = 0;
        var result_length = -1;
        if (db == null) {
            MongoClient.connect(url, function (err, database) {
                if (err) {
                    console.error('MongoDB 연결 실패', err);
                    return;
                }
                db = database;
            });
        }

        db.collection(partition).find().toArray(function (err, res) {
            result_length = res.length;
            res.forEach(function (doc) {
                cnt = cnt + 1;
                if (doc.thingIds != undefined && doc.NThingIds != undefined) {
                    doc.thingIds = doc.thingIds.sort();
                    doc.NThingIds = doc.NThingIds.sort();
                    db.collection(partition).save(doc);
                }
                else if (doc.thingIds != undefined) {
                    doc.thingIds = doc.thingIds.sort();
                    db.collection(partition).save(doc);
                }
                else if (doc.NThingIds != undefined) {
                    doc.NThingIds = doc.NThingIds.sort();
                    db.collection(partition).save(doc);
                }
            });
            if (res.length == cnt) {
                resolve5("> Partition is sorted with thingIds");
            }
        });
    });
}

exports.createAndFillPartition = createAndFillPartition;