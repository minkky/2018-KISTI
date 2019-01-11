let value = require('./value');
let async = require('async');
let fs = require('fs');
let moment = require('moment');
let partition = "Partition";
let joinCollection = [];

function setJoinCollection() {
    joinCollection = value.getJoinCollection();
}

function getQuerisToArray() {
    return new Promise(resolve => {
        let queries = [];
        for (let i = 0; i < joinCollection.length; i++) {
            queries[i] = Array.from(value.getQueries()[i]);
        }
        resolve(queries);
    });
}

function getNumberArray(query) {
    return new Promise(resolve => {
        let arr = [];
        let i = 0;
        query.forEach(function () {
            arr.push(i++);
        });
        resolve(arr);
    });
}

function appendFileWith(fileName, text) {
    return new Promise((resolve, reject) => {
        let fd;
        try {
            fd = fs.openSync(fileName, 'a');
            fs.appendFileSync(fd, text, 'utf8');
        } catch (err) {
            reject(err);
        } finally {
            if (fd !== undefined)
                fs.closeSync(fd);
            resolve(text);
        }
    });
}

function printJoinResult(results, collectionSet, fileName, startTime) {
    return new Promise(resolve => {
        let i, j, k, l, m;
        let result1, result2, result3, result4, result5;
        result1 = results[0];
        result2 = results[1];

        if (joinCollection.length >= 3) {
            result3 = results[2];
            if (joinCollection.length >= 4) {
                result4 = results[3];
                if (joinCollection.length == 5) {
                    result5 = results[4];
                }
            }
        }

        for (i = 0; i < result1.length; i++) {
            for (j = 0; j < result2.length; j++) {
                let lng_diff = Math.abs(result1[i].location[0] - result2[j].location[0]);
                let lat_diff = Math.abs(result1[i].location[1] - result2[j].location[1]);
                let t1 = toISOTime(result1[i].time, 2);
                let t2 = toISOTime(result2[j].time, 2);
                let time_diff = Math.abs(t1.diff(t2, "seconds"));
                let id1 = result1[i]._id;
                let id2 = result2[j]._id;
                let idSet = JSON.stringify([id1, id2]);
                if (lng_diff <= value.getSearchLocInterval() && lat_diff <= value.getSearchLocInterval() && time_diff <= value.getSearchTimeInterval()) {
                    if (joinCollection.length == 2) {
                        if (!collectionSet.has(idSet)) {
                            delete result1[i]._id; delete result2[j]._id;
                            let result = JSON.stringify(result1[i]) + " " + JSON.stringify(result2[j]) + "\n";
                            let time = process.hrtime(startTime);
                            collectionSet.add(idSet);
                            time = (time[0] * 1e9 + time[1]) / 1e9;
                            console.log(time);
                            appendFileWith(fileName, result)
                                .then(function (res) {
                                    //console.log(result);
                                });
                        }
                    }
                    else { // join collection 3개 이상
                        for (k = 0; k < result3.length; k++) {
                            let lng_diff1 = Math.abs(result1[i].location[0] - result3[k].location[0]);
                            let lng_diff2 = Math.abs(result2[j].location[0] - result3[k].location[0]);
                            let lat_diff1 = Math.abs(result1[i].location[1] - result3[k].location[1]);
                            let lat_diff2 = Math.abs(result2[j].location[1] - result3[k].location[1]);
                            let t3 = toISOTime(result3[k].time, 2);
                            let time_diff1 = Math.abs(t1.diff(t3, "seconds"));
                            let time_diff2 = Math.abs(t2.diff(t3, "seconds"));
                            let id3 = result3[k]._id;
                            idSet = JSON.stringify([id1, id2, id3]);
                            if (lng_diff1 <= value.getSearchLocInterval() && lng_diff2 <= value.getSearchLocInterval() && lat_diff1 <= value.getSearchLocInterval() && lat_diff2 <= value.getSearchLocInterval() && time_diff1 <= value.getSearchTimeInterval() && time_diff2 <= value.getSearchTimeInterval()) {
                                if (joinCollection.length == 3) {
                                    if (!collectionSet.has(idSet)) {
                                        delete result1[i]._id; delete result2[j]._id; delete result3[k]._id;
                                        let result = JSON.stringify(result1[i]) + " " + JSON.stringify(result2[j]) + " " + JSON.stringify(result3[k]) + "\n";
                                        let time = process.hrtime(startTime);
                                        collectionSet.add(idSet);
                                        time = (time[0] * 1e9 + time[1]) / 1e9;
                                        console.log(time);
                                        appendFileWith(fileName, result)
                                            .then(function (result) {
                                                //console.log(result);
                                            });
                                    }
                                }
                                else { // join collection 4개 이상
                                    for (l = 0; l < result4.length; l++) {
                                        let lng_diff3 = Math.abs(result1[i].location[0] - result4[l].location[0]);
                                        let lng_diff4 = Math.abs(result2[j].location[0] - result4[l].location[0]);
                                        let lng_diff5 = Math.abs(result3[k].location[0] - result4[l].location[0]);

                                        let lat_diff3 = Math.abs(result1[i].location[1] - result4[l].location[1]);
                                        let lat_diff4 = Math.abs(result2[j].location[1] - result4[l].location[1]);
                                        let lat_diff5 = Math.abs(result3[k].location[1] - result4[l].location[1]);
                                        let t4 = toISOTime(result4[l].time, 2);
                                        let time_diff3 = Math.abs(t1.diff(t4, "seconds"));
                                        let time_diff4 = Math.abs(t2.diff(t4, "seconds"));
                                        let time_diff5 = Math.abs(t3.diff(t4, "seconds"));
                                        let id4 = result4[l]._id;
                                        idSet = JSON.stringify([id1, id2, id3, id4]);
                                        if (lng_diff3 <= value.getSearchLocInterval() && lng_diff4 <= value.getSearchLocInterval() && lng_diff5 <= value.getSearchLocInterval() && lat_diff3 <= value.getSearchLocInterval() && lat_diff4 <= value.getSearchLocInterval() && lat_diff5 <= value.getSearchLocInterval() && time_diff3 <= value.getSearchTimeInterval() && time_diff4 <= value.getSearchTimeInterval() && time_diff5 <= value.getSearchTimeInterval()) {
                                            if (joinCollection.length == 4) {
                                                if (!collectionSet.has(idSet)) {
                                                    delete result1[i]._id; delete result2[j]._id; delete result3[k]._id; delete result4[l]._id;
                                                    let result = JSON.stringify(result1[i]) + " " + JSON.stringify(result2[j]) + " " + JSON.stringify(result3[k]) + " " + JSON.stringify(result4[l]) + "\n";
                                                    let time = process.hrtime(startTime);
                                                    collectionSet.add(idSet);
                                                    time = (time[0] * 1e9 + time[1]) / 1e9;
                                                    console.log(time);
                                                    appendFileWith(fileName, result)
                                                        .then(function (result) {
                                                            ;
                                                        });
                                                }
                                            }
                                            else { // join collection 5개
                                                for (m = 0; m < result5.length; m++) {
                                                    let lng_diff6 = Math.abs(result1[i].location[0] - result5[m].location[0]);
                                                    let lng_diff7 = Math.abs(result2[j].location[0] - result5[m].location[0]);
                                                    let lng_diff8 = Math.abs(result3[k].location[0] - result5[m].location[0]);
                                                    let lng_diff9 = Math.abs(result4[l].location[0] - result5[m].location[0]);

                                                    let lat_diff6 = Math.abs(result1[i].location[1] - result5[m].location[1]);
                                                    let lat_diff7 = Math.abs(result2[j].location[1] - result5[m].location[1]);
                                                    let lat_diff8 = Math.abs(result3[k].location[1] - result5[m].location[1]);
                                                    let lat_diff9 = Math.abs(result4[l].location[1] - result5[m].location[1]);

                                                    let t5 = toISOTime(result5[m].time, 2);
                                                    let time_diff6 = Math.abs(t1.diff(t5, "seconds"));
                                                    let time_diff7 = Math.abs(t2.diff(t5, "seconds"));
                                                    let time_diff8 = Math.abs(t3.diff(t5, "seconds"));
                                                    let time_diff9 = Math.abs(t4.diff(t5, "seconds"));
                                                    let id5 = result5[m]._id;
                                                    idSet = JSON.stringify([id1, id2, id3, id4, id5]);

                                                    if (lng_diff6 <= value.getSearchLocInterval() && lng_diff7 <= value.getSearchLocInterval() && lng_diff8 <= value.getSearchLocInterval() && lng_diff9 <= value.getSearchLocInterval() && lat_diff6 <= value.getSearchLocInterval() && lat_diff7 <= value.getSearchLocInterval() && lat_diff8 <= value.getSearchLocInterval() && lat_diff9 <= value.getSearchLocInterval() && time_diff6 <= value.getSearchTimeInterval() && time_diff7 <= value.getSearchTimeInterval() && time_diff8 <= value.getSearchTimeInterval() && time_diff9 <= value.getSearchTimeInterval()) {
                                                        if (!collectionSet.has(idSet)) {
                                                            delete result1[i]._id; delete result2[j]._id; delete result3[k]._id; delete result4[l]._id; delete result5[m]._id;
                                                            let result = JSON.stringify(result1[i]) + " " + JSON.stringify(result2[j]) + " " + JSON.stringify(result3[k]) + " " + JSON.stringify(result4[l]) + " " + JSON.stringify(result5[m]) + "\n";
                                                            let time = process.hrtime(startTime);
                                                            collectionSet.add(idSet);
                                                            time = (time[0] * 1e9 + time[1]) / 1e9;
                                                            console.log(time);
                                                            appendFileWith(fileName, result)
                                                                .then(function (result) {
                                                                    ;
                                                                });
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if (i == result1.length - 1 && j == result2.length - 1)
                    resolve(collectionSet);
            }
        }
    });
}

function findJoinCollections(fileName, startTime) {
    setJoinCollection();
    findJoinQueries()
        .then(function (res) {
                if (res == "doc is not exist") {
                    console.log("join 끝");
                    let time = process.hrtime(startTime);
                    time = (time[0] * 1e9 + time[1]) / 1e9;
                    console.log(time);
                }
                else {
                    let collectionSet = new Set();

                    getQuerisToArray()
                        .then(function (queries) {
                            getNumberArray(queries[0])
                                .then(function (query) {
                                    for (let j = 0; j < query.length; j++) {
                                        let i = 0;
                                        (function (j) {
                                            async.waterfall([
                                                    function (callback) {
                                                        getResultWith(queries[i][j], joinCollection[i])
                                                            .then(function (result) {
                                                                callback(null, result);
                                                            });
                                                    },
                                                    function (result1, callback) {
                                                        getResultWith(queries[i + 1][j], joinCollection[i + 1])
                                                            .then(function (result) {
                                                                callback(null, result1, result);
                                                            });
                                                    },
                                                    function (result1, result2, callback) {
                                                        if (joinCollection.length >= 3) {
                                                            getResultWith(queries[i + 2][j], joinCollection[i + 2])
                                                                .then(function (result) {
                                                                    callback(null, result1, result2, result);
                                                                });
                                                        }
                                                        else
                                                            callback(null, result1, result2, undefined);
                                                    },
                                                    function (result1, result2, result3, callback) {
                                                        if (joinCollection.length >= 4) {
                                                            getResultWith(queries[i + 3][j], joinCollection[i + 3])
                                                                .then(function (result) {
                                                                    callback(null, result1, result2, result3, result);
                                                                });
                                                        }
                                                        else
                                                            callback(null, result1, result2, result3, undefined);
                                                    },
                                                    function (result1, result2, result3, result4, callback) {
                                                        if (joinCollection.length >= 5) {
                                                            getResultWith(queries[i + 4][j], joinCollection[i + 4])
                                                                .then(function (result) {
                                                                    callback(null, result1, result2, result3, result4, result);
                                                                });
                                                        }
                                                        else
                                                            callback(null, result1, result2, result3, result4, undefined);
                                                    },
                                                    function (result1, result2, result3, result4, result5, callback) {
                                                        let results = [];
                                                        results.push(result1);
                                                        results.push(result2);
                                                        if (joinCollection.length >= 3) {
                                                            results.push(result3);
                                                            if (joinCollection.length >= 4) {
                                                                results.push(result4);
                                                                if (joinCollection.length == 5)
                                                                    results.push(result5);
                                                            }
                                                        }
                                                        callback(null, results);
                                                    }],
                                                function (err, results) {
                                                    if (err) ;
                                                    printJoinResult(results, collectionSet, fileName, startTime)
                                                        .then(function (result) {
                                                            if (j == query.length - 1) {
                                                                console.log("join 끝");
                                                                let time = process.hrtime(startTime);
                                                                time = (time[0] * 1e9 + time[1]) / 1e9;
                                                                console.log(time);
                                                                /*appendFileWith(fileName, time + "\n")
                                                                    .then(function (res) {
                                                                        ;
                                                                    });*/
                                                            }
                                                        });
                                                });
                                        })(j);
                                    }
                                });
                        });
                }
            }
        );
}

function getResultWith(query, collectionName) {
    return new Promise(resolve => {
        value.getDB().collection(collectionName).find(query).sort({_id: 1}).toArray(function (err, result) {
            if (err) throw err;
            resolve(result);
        });
    });
}

function getPartitionXLength() {
    let size = value.getSMax().toString();
    size = size.split('.');
    size = size[1].length;
    return size;
}

function getRealLocation(index, minValue) {
    return new Promise(resolve => {
        let len = getPartitionXLength();
        let loc = index * value.getXSize() + minValue;
        resolve(Number(loc.toFixed(len)));
    });
}

function createBoxQuery(range, lteTime, gteTime) {
    return {
        location: {
            $geoWithin: {
                $box: range
            }
        },
        time: {$gte: gteTime, $lte: lteTime}
    }
}

function sToMin(sec) {
    return sec / 60;
}

function getORQuery() {
    return new Promise(resolve => {
        let orQuery = [];
        for (i = 0; i < joinCollection.length; i++) {
            orQuery.push({$or: [{thingIds: joinCollection[i]}, {NThingIds: joinCollection[i]}]});
        }
        return resolve(orQuery);
    });
}

function getFullQuery() {
    return new Promise(resolve => {
        getORQuery()
            .then(function (query) {
                let fullQuery = {
                    $and: query
                }
                resolve(fullQuery);
            })
    });
}

let findJoinQueries = function () {
    return new Promise(resolve => {
            let queryArray = [];
            for (let i = 0; i < joinCollection.length; i++) {
                queryArray[i] = new Set();
            }

            getFullQuery()
                .then(function (fullquery) {
                    let query = fullquery;
                    value.getDB().collection(partition).find(query).toArray(function (err, result) {
                        return new Promise(resolve1 => {
                            let len = result.length;
                            let cnt = 0;
                            if (result.length == 0) {
                                resolve1("doc is not exist");
                            }
                            else {
                                result.forEach(function (doc) {
                                    if (doc) {
                                        async.waterfall([
                                            function (callback) {
                                                let nthings = doc.NThingIds;
                                                let cases = [];
                                                let BOUND = 0;
                                                let NEAR = 1;
                                                for (let i = 0; i < joinCollection.length; i++) {
                                                    if (nthings != undefined && nthings.includes(joinCollection[i]))
                                                        cases.push(NEAR);
                                                    else
                                                        cases.push(BOUND);
                                                }
                                                callback(null, cases);
                                            },
                                            function (cases, callback) {
                                                let lng_left, lng_right, lat_left, lat_right;
                                                getRealLocation(doc.x_index - 1, value.getMinLNG())
                                                    .then(function (llng) {
                                                        return new Promise(resolve2 => {
                                                            lng_left = llng;
                                                            getRealLocation(doc.x_index, value.getMinLNG())
                                                                .then(function (rlng) {
                                                                    lng_right = rlng;
                                                                    getRealLocation(doc.y_index - 1, value.getMinLAT())
                                                                        .then(function (llat) {
                                                                            lat_left = llat;
                                                                            getRealLocation(doc.y_index, value.getMinLAT())
                                                                                .then(function (rlat) {
                                                                                    lat_right = rlat;
                                                                                    resolve2("commit");
                                                                                });
                                                                        });
                                                                });
                                                        })
                                                            .then(function (res) {
                                                                let range = [[lng_left, lat_left], [lng_right, lat_right]];
                                                                let nlng_left = lng_left;
                                                                let nlng_right = lng_right;
                                                                let nlat_left = lat_left;
                                                                let nlat_right = lat_right;
                                                                let nrange;
                                                                if (cases.includes(1)) {
                                                                    async.waterfall([
                                                                            function (callback) {
                                                                                let len = getPartitionXLength();
                                                                                nlng_left -= value.getSMax();
                                                                                nlat_left -= value.getSMax();
                                                                                nlng_right += value.getSMax();
                                                                                nlat_right += value.getSMax();
                                                                                callback(null, len, Number(nlng_left), Number(nlat_left), Number(nlng_right), Number(nlat_right));
                                                                            },
                                                                            function (len, nlng_left, nlat_left, nlng_right, nlat_right, callback) {
                                                                                nlng_left = Number(nlng_left.toFixed(len));
                                                                                nlat_left = Number(nlat_left.toFixed(len));
                                                                                nlng_right = Number(nlng_right.toFixed(len));
                                                                                nlat_right = Number(nlat_right.toFixed(len));
                                                                                callback(null, nlng_left, nlat_left, nlng_right, nlat_right);
                                                                            },
                                                                            function (nlng_left, nlat_left, nlng_right, nlat_right, callback) {
                                                                                nrange = [[nlng_left, nlat_left], [nlng_right, nlat_right]];
                                                                                callback(null, range, nrange, cases);
                                                                            }
                                                                        ],
                                                                        function (err, range, nrange, cases) {
                                                                            callback(null, range, nrange, cases);
                                                                        });
                                                                }
                                                                else {
                                                                    nrange = range;
                                                                    callback(null, range, nrange, cases);
                                                                }
                                                            });
                                                    });
                                            },
                                            function (range, nrange, cases, callback) {
                                                let time = value.getMinTIME().toISOString().split('.')[0];
                                                let leftMinutes = sToMin(value.getTSize()) * (doc.t_index - 1);
                                                let rightMinutes = sToMin(value.getTSize()) * doc.t_index;
                                                let gteTime = moment(time).add(leftMinutes, "minutes").format('YYYY-MM-DD HH:mm:ss').toString();
                                                let lteTime = moment(time).add(rightMinutes, "minutes").format('YYYY-MM-DD HH:mm:ss').toString();
                                                let ngteTime = moment(time).add(leftMinutes, "minutes").add(-1 * value.getTMax(), "seconds").format('YYYY-MM-DD HH:mm:ss').toString();
                                                let nlteTime = moment(time).add(rightMinutes, "minutes").add(value.getTMax(), "seconds").format('YYYY-MM-DD HH:mm:ss').toString();
                                                callback(null, lteTime, gteTime, range, nrange, ngteTime, nlteTime, cases);
                                            },
                                            function (lteTime, gteTime, range, nrange, ngteTime, nlteTime, cases, callback) {
                                                let query = createBoxQuery(range, toISOTime(lteTime, 1), toISOTime(gteTime, 1));
                                                let nquery = createBoxQuery(nrange, toISOTime(nlteTime, 1), toISOTime(ngteTime, 1));
                                                callback(null, query, nquery, cases);
                                            },
                                            function (query, nquery, cases, callback) {
                                                let i = 0;
                                                cases.forEach(element => {
                                                    if(element == 0)
                                                        queryArray[i++].add(query);
                                                    else
                                                        queryArray[i++].add(nquery);
                                                });
                                                callback(null, queryArray);
                                            }
                                        ], function (err, result) {
                                            cnt = cnt + 1;
                                            if (cnt == len) {
                                                value.setQueries(queryArray);
                                                resolve1(doc);
                                            }
                                        });
                                    }
                                });
                            }
                        })
                            .then(function (doc) {
                                if (doc == "doc is not exist")
                                    resolve(doc);
                                else resolve(queryArray);
                            });
                    });
                });
        }
    )
        ;
}

function toISOTime(time, status) {
    let front, end;
    if (status == 1) {
        front = time.split(' ')[0];
        end = time.split(' ')[1];
        let year = parseInt(front.split('-')[0]);
        let month = parseInt(front.split('-')[1]);
        let day = parseInt(front.split('-')[2]);
        let hour = parseInt(end.split(':')[0]);
        let min = parseInt(end.split(':')[1]);
        let ss = parseInt(end.split(':')[2]);
        let date = new Date(Date.UTC(year, month - 1, day, hour, min, ss));
        return moment(date).toISOString();
    }
    else {
        let tt = new Date(time);
        let yy = tt.getUTCFullYear();
        let mon = tt.getUTCMonth() + 1;
        let dd = tt.getUTCDate();
        let hh = tt.getUTCHours();
        let mm = tt.getUTCMinutes();
        let sec = tt.getUTCSeconds();
        let utc = new Date(Date.UTC(yy, mon - 1, dd, hh, mm, sec));
        return moment(utc);
    }
}

exports.appendFileWith = appendFileWith;
exports.findJoinCollections = findJoinCollections;