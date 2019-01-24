let express = require('express');
let async = require('async');
let router = express.Router();
let fs = require('fs');
let path = require('path');
let loadData = require('./loadData');
let searchFunc = require('../public/function/search');
let indexFunc = require('../public/function/myIndex');
let value = require('../public/function/value');
let moment = require('moment');
let count_join_collection = 2;
let collectionName1 = "IK1003";
let collectionName2 = "IK1008";
let collectionName3;
let collectionName4;
let collectionName5;
let within_time;
let within_distance;
let MongoClient = require('mongodb').MongoClient;
let url = "mongodb://localhost:27017/KISTI";
let exec = require('child_process').exec;
let collections = [];

function getCollectionList() {
    return new Promise(resolve => {
        let i = 0;
        collections = [];
        value.connect()
            .then(function (db) {
                db.listCollections().toArray(function (err, names) {
                    if (err) console.error(err);
                    else {
                        while (collections.length < names.length) {
                            collections.push(names[i++].name);
                        }
                        collections = collections.sort();
                    }
                    resolve(collections);
                });
            });
    });
}

/* GET home page. */
router.get('/', function (req, res, next) {
    getCollectionList()
        .then(function (collections) {
            if (collections.length > 0)
                res.render('index', {title: 'KISTI-Prototype', result: collections, len: collections.length});
            else
                res.render('index', {title: 'KISTI-Prototype', result: undefined, len: 0});
        });
});

router.get('/buttonClicked', function (req, res, next) {
    let load = function (param) {
        return new Promise(function (resolve, reject) {
            loadData.load_data1(collectionName1);
            if (param)
                resolve("true");
            else
                reject("false");
        })
    };

    load(true)
        .then(function () {
            loadData.load_data2(collectionName2);
        }, function (err) {
            console.log("error ", err.code);
        });

    res.redirect('/');
});

router.post('/search', function (req, res, next) {
    async.waterfall([
            function (callback) {
                collectionName3 = undefined;
                collectionName4 = undefined;
                collectionName5 = undefined;
                callback(null, 1)
            },
            function (v, callback) {
                console.log("search btn clicked");
                let resultPath = path.join(process.cwd(), "public/result");
                let type = req.body.submit;
                let taxi_id = [];
                within_time = Number(req.body.within_time);
                within_distance = Number(req.body.within_distance);
                modifyJoinCollection(req.body.taxi_id)
                    .then(function (result) {
                        taxi_id = result;
                        callback(null, resultPath, type, taxi_id, within_time, within_distance);
                    });
            },
            function (resultPath, type, taxi_id, within_time, within_distance, callback) {
                let c = [];
                taxi_id.forEach(function (id) {
                    indexFunc.createLocationTimeIndex(id)
                        .then(function (res) {
                            if (res) {
                                c.push(id);
                            }
                            if (c.length == taxi_id.length) {
                                c = null;
                                callback(null, resultPath, type, taxi_id, within_time, within_distance);
                            }
                        });
                });
            },
            function (resultPath, type, taxi_id, within_time, within_distance, callback) {
                let ids = "";
                for (let i = 0; i < taxi_id.length; i++) {
                    ids += taxi_id[i] + "_";
                    if (i == taxi_id.length - 1)
                        callback(null, resultPath, type, taxi_id, within_time, within_distance, ids);
                }
            },
            function (resultPath, type, taxi_id, within_time, within_distance, ids, callback) {
                global.done = "no";
                let fileName = path.join(resultPath, "result" + ids + within_time + "_" + within_distance + ".txt");
                callback(null, fileName, type, within_time, within_distance, taxi_id.sort(), ids);
            },
            function (fileName, type, within_time, within_distance, taxi_id, ids, callback) {
                fs.exists(fileName, function (exists) {
                    if (exists) {
                        fs.unlink(fileName, (err) => {
                            if (err) throw  err;
                            console.log(fileName + " removed");
                            callback(null, fileName, type, within_time, within_distance, taxi_id, ids);
                        });
                    }
                    else {
                        console.log(fileName + " not exist");
                        callback(null, fileName, type, within_time, within_distance, taxi_id, ids);
                    }
                });
            }
        ],
        function (err, fileName, type, within_time, within_distance, taxi_id, ids) {
            if (err) ;
            if (type == "search") {
                let startTime = process.hrtime();
                INLSearch(within_time, within_distance, taxi_id.sort(), fileName)
                    .then(function (result) {
                        console.log(result);
                        let fileLink = "http://localhost:3000/"+ "result/result" + ids + within_time + "_"+ within_distance + ".txt";
                        res.render('searchResult', {
                            mod: 'base',
                            title: 'Join Result - base ver.',
                            within_time: within_time,
                            within_distance: within_distance,
                            fileLink : fileLink
                        });
                    });
            }
        });
});

function settingCollectionName(id) {
    return new Promise(resolve => {
        collectionName1 = id[0];
        collectionName2 = id[1];
        if (id.length >= 3) {
            collectionName3 = id[2];
            if (id.length >= 4) {
                collectionName4 = id[3];
                if (id.length == 5) {
                    collectionName5 = id[4];
                    resolve(id.length);
                }
                else
                    resolve(id.length);
            }
            else {
                resolve(id.length);
            }
        }
        else {
            resolve(id.length);
        }
    });
}

function getSignificant_figures(number) {
    return number.toString().length - number.toString().indexOf(".") - 1;
}

function floor(number, sig) {
    let p = Math.pow(10, sig);
    let num = Math.floor(number * p) / p;
    return num;
}

function getQuery(res) {
    return new Promise(resolve => {
        let lng = res.location[0];
        let lat = res.location[1];
        let sig_lng = getSignificant_figures(lng);
        let sig_lat = getSignificant_figures(lat);
        let time = res.time;
        time = new Date(time);
        time.setHours(time.getHours() + 9);
        let year = time.getUTCFullYear();
        let mon = time.getUTCMonth() + 1;
        let date = time.getUTCDate();
        let hour = time.getUTCHours();
        let min = time.getUTCMinutes();
        let sec = time.getUTCSeconds();
        let utc = new Date(Date.UTC(year, mon - 1, date, hour, min, sec));
        let gte_time = moment(utc).add(-1 * within_time, 'm').toISOString();
        let lte_time = moment(utc).add(within_time, 'm').toISOString();

        let query = {
            $and: [
                {$and: [{"location.0": {$gte: floor(lng - within_distance, sig_lng)}}, {"location.0": {$lte: floor(lng + within_distance, sig_lng)}}]},
                {$and: [{"location.1": {$gte: floor(lat - within_distance, sig_lat)}}, {"location.1": {$lte: floor(lat + within_distance, sig_lat)}}]},
                {time: {$gte: gte_time, $lte: lte_time}}
            ]
        };
        //console.log(JSON.stringify(query1))
        /*let query = {
            location: {
                $geoWithin: {
                    $centerSphere: [[Number(lng), Number(lat)], radius_dis]
                }
            },
            time: {$gte: gte, $lte: lte}
        };*/
        resolve(query);
    });
}

function modifyJoinCollection(taxi_id) {
    return new Promise(resolve => {
        let i = 0;
        let taxi_collection = taxi_id.split(",");
        if (taxi_collection.length == 0) {
            taxi_collection = [collectionName1, collectionName2];
            count_join_collection = 2;
            resolve(taxi_collection);
        }
        else {
            count_join_collection = taxi_collection.length;
            taxi_collection.forEach(collection => {
                taxi_collection[i++] = collection.replace(/(\s*)/g, "").toUpperCase();
                if (i == taxi_collection.length)
                    resolve(taxi_collection);
            });
        }
    })
}

function getResultFromCollection(database, collectionName, query) {
    return new Promise(resolve => {
        database.collection(collectionName).find(query).toArray(function (err, result) {
            resolve(result);
        });
    });
}

function INLSearch(within_time, within_distance, taxi_id, fileName) {
    return new Promise(resolve => {
        let startTime = process.hrtime();
        count_join_collection = taxi_id.length;
        settingCollectionName(taxi_id)
            .then(function (result) {
                console.log(count_join_collection, collectionName1, collectionName2, collectionName3, collectionName4, collectionName5);
            });

        MongoClient.connect(url, function (err, database) {
            if (err) throw err;

            database.collection(collectionName1).find().toArray(function (err, result) {
                let two = 0, three = 0, four = 0, five = 0;
                let count2 = 0, count3 = 0, count4 = 0;
                result.forEach(function (res1) {
                    let query1 = {};
                    getQuery(res1)
                        .then(function (que) {
                            query1 = que;
                            database.collection(collectionName2).find(query1).toArray(function (err, result2) {
                                if (count_join_collection == 2) {
                                    if (result2.length != 0) {
                                        saveFileWithTwoJoinResult(fileName, res1, result2)
                                            .then(function (res) {
                                                if (++two == result.length) {
                                                    let time = process.hrtime(startTime);
                                                    time = (time[0] * 1e9 + time[1]) / 1e9;
                                                    console.log(time);
                                                    appendFileSeparate(fileName, time + "\n")
                                                        .then(function (res) {
                                                            resolve("INL Finished!");
                                                        });
                                                }
                                            });
                                    }
                                    else {
                                        if (++two == result.length) {
                                            let time = process.hrtime(startTime);
                                            time = (time[0] * 1e9 + time[1]) / 1e9;
                                            console.log(time);
                                            appendFileSeparate(fileName, time + "\n")
                                                .then(function (res) {
                                                    resolve("INL Finished!");
                                                });
                                        }
                                    }
                                }
                                else if (count_join_collection >= 3) {
                                    if (result2.length != 0) {
                                        let three_in = 0;
                                        ++three;
                                        ++four;
                                        ++five;
                                        count2 += result2.length;
                                        result2.forEach(function (res2) {
                                            let query2 = {};
                                            getQuery(res2)
                                                .then(function (que) {
                                                    query2 = que;
                                                    let result3_1 = [];
                                                    let result3_2 = [];
                                                    async.waterfall([
                                                            function (callback) {
                                                                getResultFromCollection(database, collectionName3, query1)
                                                                    .then(function (res) {
                                                                        result3_1 = res;
                                                                        callback(null, result3_1);
                                                                    });
                                                            },
                                                            function (result3_1, callback) {
                                                                getResultFromCollection(database, collectionName3, query2)
                                                                    .then(function (res) {
                                                                        result3_2 = res;
                                                                        callback(null, result3_1, result3_2);
                                                                    });
                                                            },
                                                            function (result3_1, result3_2, callback) {
                                                                if (result3_1.length != 0 && result3_2.length != 0) {
                                                                    getIntersect2(result3_1, result3_2)
                                                                        .then(function (intersect3) {
                                                                            callback(null, result3_1, Array.from(intersect3));
                                                                        });
                                                                }
                                                                else {
                                                                    callback(null, result3_1, []);
                                                                }
                                                            },
                                                            function (both_result, intersect3, callback) {
                                                                if (intersect3.length != 0) {
                                                                    let result3 = [];
                                                                    both_result.forEach(function (res) {
                                                                        if (intersect3.includes((res._id).toString()))
                                                                            result3.push(res);
                                                                    });
                                                                    callback(null, result3);
                                                                }
                                                                else
                                                                    callback(null, []);
                                                            }
                                                        ],
                                                        function (err, result3) {
                                                            if (count_join_collection == 3) {
                                                                saveFileWithThrJoinResult(fileName, res1, res2, result3)
                                                                    .then(function (res) {
                                                                        three++;
                                                                        if (res) {
                                                                            if (++three_in == result2.length && three == result.length + count2) {
                                                                                let time = process.hrtime(startTime);
                                                                                time = (time[0] * 1e9 + time[1]) / 1e9;
                                                                                console.log(time);
                                                                                appendFileSeparate(fileName, time)
                                                                                    .then(function (res) {
                                                                                        resolve("INL Finished!");
                                                                                    });
                                                                            }
                                                                        }
                                                                    });
                                                            }
                                                            else { // collection 4, 5
                                                                let four_in = 0;
                                                                if (result3.length != 0) {
                                                                    count3 += result3.length;
                                                                    result3.forEach(function (res3) {
                                                                        let query3 = {};
                                                                        getQuery(res3)
                                                                            .then(function (que) {
                                                                                query3 = que;
                                                                                let result4_1 = [];
                                                                                let result4_2 = [];
                                                                                let result4_3 = [];
                                                                                async.waterfall([
                                                                                        function (callback) {
                                                                                            getResultFromCollection(database, collectionName4, query1)
                                                                                                .then(function (result) {
                                                                                                    result4_1 = result;
                                                                                                    callback(null, result4_1);
                                                                                                });
                                                                                        },
                                                                                        function (result4_1, callback) {
                                                                                            getResultFromCollection(database, collectionName4, query2)
                                                                                                .then(function (result) {
                                                                                                    result4_2 = result;
                                                                                                    callback(null, result4_1, result4_2);
                                                                                                });
                                                                                        },
                                                                                        function (result4_1, result4_2, callback) {
                                                                                            getResultFromCollection(database, collectionName4, query3)
                                                                                                .then(function (result) {
                                                                                                    result4_3 = result;
                                                                                                    callback(null, result4_1, result4_2, result4_3);
                                                                                                });
                                                                                        },
                                                                                        function (result4_1, result4_2, result4_3, callback) {
                                                                                            if (result4_1.length > 0 && result4_2.length > 0 && result4_3.length > 0) {
                                                                                                getIntersect3(result4_1, result4_2, result4_3)
                                                                                                    .then(function (intersect4) {
                                                                                                        callback(null, result4_1, Array.from(intersect4));
                                                                                                    });
                                                                                            }
                                                                                            else
                                                                                                callback(null, result4_1, []);
                                                                                        },
                                                                                        function (both_result, intersect4, callback) {
                                                                                            if (intersect4.length != 0) {
                                                                                                let result4 = [];
                                                                                                both_result.forEach(function (res) {
                                                                                                    if (intersect4.includes((res._id).toString()))
                                                                                                        result4.push(res);
                                                                                                });
                                                                                                callback(null, result4);
                                                                                            }
                                                                                            else
                                                                                                callback(null, []);
                                                                                        }
                                                                                    ],
                                                                                    function (err, result4) {
                                                                                        if (err) throw err;
                                                                                        if (result4) {
                                                                                            if (count_join_collection == 4) {
                                                                                                saveFileWithFourJoinResult(fileName, res1, res2, res3, result4)
                                                                                                    .then(function (res) {
                                                                                                        ++four;
                                                                                                        if (res) {
                                                                                                            if (++four_in == result3.length && four == result.length + count3) {
                                                                                                                let time = process.hrtime(startTime);
                                                                                                                time = (time[0] * 1e9 + time[1]) / 1e9;
                                                                                                                console.log(time);
                                                                                                                appendFileSeparate(fileName, time)
                                                                                                                    .then(function (res) {
                                                                                                                        resolve("INL Finished!");
                                                                                                                    });
                                                                                                            }
                                                                                                        }
                                                                                                    });
                                                                                            }
                                                                                            else if (count_join_collection == 5) {
                                                                                                let five_in = 0;
                                                                                                if (result4.length != 0) {
                                                                                                    count4 += result4.length;
                                                                                                    result4.forEach(function (res4) {
                                                                                                        let query4 = {};
                                                                                                        getQuery(res4)
                                                                                                            .then(function (que) {
                                                                                                                query4 = que;
                                                                                                                let result5_1 = [];
                                                                                                                let result5_2 = [];
                                                                                                                let result5_3 = [];
                                                                                                                let result5_4 = [];
                                                                                                                async.waterfall([
                                                                                                                        function (callback) {
                                                                                                                            getResultFromCollection(database, collectionName5, query1)
                                                                                                                                .then(function (res) {
                                                                                                                                    result5_1 = res;
                                                                                                                                    callback(null, result5_1);
                                                                                                                                });
                                                                                                                        },
                                                                                                                        function (result5_1, callback) {
                                                                                                                            getResultFromCollection(database, collectionName5, query2)
                                                                                                                                .then(function (res) {
                                                                                                                                    result5_2 = res;
                                                                                                                                    callback(null, result5_1, result5_2);
                                                                                                                                });
                                                                                                                        },
                                                                                                                        function (result5_1, result5_2, callback) {
                                                                                                                            getResultFromCollection(database, collectionName5, query3)
                                                                                                                                .then(function (res) {
                                                                                                                                    result5_3 = res;
                                                                                                                                    callback(null, result5_1, result5_2, result5_3);
                                                                                                                                });
                                                                                                                        },
                                                                                                                        function (result5_1, result5_2, result5_3, callback) {
                                                                                                                            getResultFromCollection(database, collectionName5, query4)
                                                                                                                                .then(function (res) {
                                                                                                                                    result5_4 = res;
                                                                                                                                    callback(null, result5_1, result5_2, result5_3, result5_4);
                                                                                                                                });
                                                                                                                        },
                                                                                                                        function (result5_1, result5_2, result5_3, result5_4, callback) {
                                                                                                                            if (result5_1.length > 0 && result5_2.length > 0 && result5_3.length > 0 && result5_4.length > 0) {
                                                                                                                                getIntersect4(result5_1, result5_2, result5_3, result5_4)
                                                                                                                                    .then(function (intersect5) {
                                                                                                                                        callback(null, result5_1, Array.from(intersect5));
                                                                                                                                    });
                                                                                                                            }
                                                                                                                            else
                                                                                                                                callback(null, result5_1, []);
                                                                                                                        },
                                                                                                                        function (both_result, intersect5, callback) {
                                                                                                                            if (intersect5.length != 0) {
                                                                                                                                let result5 = [];
                                                                                                                                both_result.forEach(function (res) {
                                                                                                                                    if (intersect5.includes((res._id).toString()))
                                                                                                                                        result5.push(res);
                                                                                                                                });
                                                                                                                                callback(null, result5);
                                                                                                                            }
                                                                                                                            else
                                                                                                                                callback(null, []);
                                                                                                                        }
                                                                                                                    ],
                                                                                                                    function (err, res5) {
                                                                                                                        if (err) throw err;
                                                                                                                        if (res5) {
                                                                                                                            saveFileWithFiveJoinResult(fileName, res1, res2, res3, res4, res5)
                                                                                                                                .then(function (res) {
                                                                                                                                    ++five;
                                                                                                                                    if (res) {
                                                                                                                                        if (result4.length == ++five_in && five == result.length + count4) {
                                                                                                                                            let time = process.hrtime(startTime);
                                                                                                                                            time = (time[0] * 1e9 + time[1]) / 1e9;
                                                                                                                                            console.log(time);
                                                                                                                                            appendFileSeparate(fileName, time)
                                                                                                                                                .then(function (res) {
                                                                                                                                                    resolve("INL Finished!");
                                                                                                                                                });
                                                                                                                                        }
                                                                                                                                    }
                                                                                                                                });
                                                                                                                        }
                                                                                                                    });
                                                                                                            });
                                                                                                    });
                                                                                                }
                                                                                                else {
                                                                                                    if (five == result.length && count_join_collection == 5) {
                                                                                                        resolve("INL Finished!");
                                                                                                        let time = process.hrtime(startTime);
                                                                                                        time = (time[0] * 1e9 + time[1]) / 1e9;
                                                                                                        console.log(time);
                                                                                                    }
                                                                                                }
                                                                                            }
                                                                                        }
                                                                                    });
                                                                            });
                                                                    });
                                                                }
                                                                else {
                                                                    if (four == result.length && count_join_collection == 4) {
                                                                        resolve("INL Finished!");
                                                                        let time = process.hrtime(startTime);
                                                                        time = (time[0] * 1e9 + time[1]) / 1e9;
                                                                        console.log(time);
                                                                    }
                                                                }
                                                            }
                                                        });
                                                });
                                        });
                                    }
                                    else {
                                        ++three;
                                        ++four;
                                        ++five;
                                        if (three == result.length && count_join_collection == 3) {
                                            resolve("INL Finished!");
                                            let time = process.hrtime(startTime);
                                            time = (time[0] * 1e9 + time[1]) / 1e9;
                                            console.log(time);
                                        }
                                    }
                                }
                            });
                        });
                });
            });
        });
    });
}

function saveFileWithTwoJoinResult(fileName, res1, res2) {
    return new Promise(resolve1 => {
        let i = 0;
        res2.forEach(function (result2) {
            return new Promise(resolve => {
                let result = JSON.stringify(res1) + " " + JSON.stringify(result2) + "\n";
                appendFileSeparate(fileName, result)
                    .then(function (res1) {
                        i++;
                        resolve(res1);
                        if (i == res2.length)
                            resolve1(i);
                    });
            });
        });
        if (res2.length == 0)
            resolve1(1);
    });
}

function saveFileWithThrJoinResult(fileName, res1, res2, res3) {
    return new Promise(resolve => {
        let i = 0;
        res3.forEach(function (result3) {
            return new Promise(resolve1 => {
                let result = JSON.stringify(res1) + " " + JSON.stringify(res2) + " " + JSON.stringify(result3) + "\n";
                appendFileSeparate(fileName, result)
                    .then(function (res1) {
                        if (res1) {
                            i++;
                            resolve1(res1);
                        }
                        if (i == res3.length)
                            resolve(i);
                    });
            });
        });
        if (res3.length == 0)
            resolve(1);
    });
}

function saveFileWithFourJoinResult(fileName, res1, res2, res3, res4) {
    return new Promise(resolve => {
        let i = 0;
        res4.forEach(function (result4) {
            return new Promise(resolve1 => {
                let result = JSON.stringify(res1) + " " + JSON.stringify(res2) + " " + JSON.stringify(res3) + " " + JSON.stringify(result4) + "\n";
                appendFileSeparate(fileName, result)
                    .then(function (res1) {
                        if (res1) {
                            i++;
                            resolve1(res1);
                        }
                        if (i == res4.length)
                            resolve(i);
                    });
            });
        });
        if (res4.length == 0)
            resolve(1);
    });
}

function saveFileWithFiveJoinResult(fileName, res1, res2, res3, res4, res5) {
    return new Promise(resolve => {
        let i = 0;
        res5.forEach(function (result5) {
            return new Promise(resolve1 => {
                let result = JSON.stringify(res1) + " " + JSON.stringify(res2) + " " + JSON.stringify(res3) + " " + JSON.stringify(res4) + " " + JSON.stringify(result5) + "\n";
                appendFileSeparate(fileName, result)
                    .then(function (res1) {
                        if (res1) {
                            i++;
                            resolve1(res1);
                        }
                        if (i == res5.length)
                            resolve(i);
                    });
            });
        });
    });
}

function appendFileSeparate(fileName, res1) {
    return new Promise((resolve, reject) => {
        let fd;
        try {
            fd = fs.openSync(fileName, 'a');
            fs.appendFileSync(fd, res1, 'utf8');
        } catch (err) {
            reject(err);
        } finally {
            if (fd !== undefined)
                fs.closeSync(fd);
            resolve(res1);
        }
    });
}

function getIntersect2(result1, result2) {
    return new Promise(resolve => {
        let result3_id1 = [];
        let result3_id2 = [];
        async.waterfall([
            function (callback) {
                result1.forEach(function (res) {
                    result3_id1.push(res._id.toString());
                    if (result3_id1.length == result1.length)
                        callback(null, result3_id1);
                });
            },
            function (result3_id1, callback) {
                result2.forEach(function (res) {
                    result3_id2.push(res._id.toString());
                    if (result3_id2.length == result2.length)
                        callback(null, result3_id1, result3_id2);
                });
            }
        ], function (err, result3_id1, result3_id2) {
            let set1 = new Set(result3_id1);
            let set2 = new Set(result3_id2);
            let intersect = new Set([...set1].filter(x => set2.has(x)));
            resolve(intersect);
        });
    });
}

function getIntersect3(result1, result2, result3) {
    return new Promise(resolve => {
        let result4_id1 = [];
        let result4_id2 = [];
        let result4_id3 = [];
        async.waterfall([
            function (callback) {
                result1.forEach(function (res) {
                    result4_id1.push(res._id.toString());
                    if (result4_id1.length == result1.length)
                        callback(null, result4_id1);
                });
            },
            function (result4_id1, callback) {
                result2.forEach(function (res) {
                    result4_id2.push(res._id.toString());
                    if (result4_id2.length == result2.length)
                        callback(null, result4_id1, result4_id2);
                });
            },
            function (result4_id1, result4_id2, callback) {
                result3.forEach(function (res) {
                    result4_id3.push(res._id.toString());
                    if (result4_id3.length == result3.length)
                        callback(null, result4_id1, result4_id2, result4_id3);
                });
            }
        ], function (err, result4_id1, result4_id2, result4_id3) {
            let set1 = new Set(result4_id1);
            let set2 = new Set(result4_id2);
            let set3 = new Set(result4_id3);
            let intersect1 = new Set([...set1].filter(x => set2.has(x)));
            let intersect2 = new Set([...set3].filter(x => intersect1.has(x)));
            resolve(intersect2);
        });
    });
}

function getIntersect4(result1, result2, result3, result4) {
    return new Promise(resolve => {
        let result5_id1 = [];
        let result5_id2 = [];
        let result5_id3 = [];
        let result5_id4 = [];

        async.waterfall([
            function (callback) {
                result1.forEach(function (res) {
                    result5_id1.push(res._id.toString());
                    if (result5_id1.length == result1.length)
                        callback(null, result5_id1);
                });
            },
            function (result5_id1, callback) {
                result2.forEach(function (res) {
                    result5_id2.push(res._id.toString());
                    if (result5_id2.length == result2.length)
                        callback(null, result5_id1, result5_id2);
                });
            },
            function (result5_id1, result5_id2, callback) {
                result3.forEach(function (res) {
                    result5_id3.push(res._id.toString());
                    if (result5_id3.length == result3.length)
                        callback(null, result5_id1, result5_id2, result5_id3);
                });
            },
            function (result5_id1, result5_id2, result5_id3, callback) {
                result4.forEach(function (res) {
                    result5_id4.push(res._id.toString());
                    if (result5_id4.length == result4.length)
                        callback(null, result5_id1, result5_id2, result5_id3, result5_id4)
                });
            }
        ], function (err, result5_id1, result5_id2, result5_id3, result5_id4) {
            let set1 = new Set(result5_id1);
            let set2 = new Set(result5_id2);
            let set3 = new Set(result5_id3);
            let set4 = new Set(result5_id4);
            let intersect1 = new Set([...set1].filter(x => set2.has(x)));
            let intersect2 = new Set([...set3].filter(x => intersect1.has(x)));
            let intersect3 = new Set([...set4].filter(x => intersect2.has(x)));
            resolve(intersect3);
        });
    });
}

let lookupSearch = function (within_time, within_distance, res, taxi_id, startTime, resultPath) {
    let join_result = [];

    MongoClient.connect(url)
        .then(function (db) {
            let count = 0;
            let join_count = 0;
            let total = 0;

            db.collection(collectionName1).find().toArray(function (err, result) {
                if (err) throw err;
                let len = result.length;
                result.forEach(function (doc) {
                    async.waterfall([
                            function (callback) {
                                let test = JSON.stringify(doc);
                                let loc = searchFunc.splitLocation(test);
                                let lng = loc.split(',')[0];
                                let lat = loc.split(',')[1];
                                let time = searchFunc.splitTime(test);
                                let extra = searchFunc.splitExtraInfo(test);
                                let date = time.split('T')[0].split('-');
                                let tt = time.split('T')[1].split(':');
                                let year = Number(date[0]), mon = Number(date[1]), dd = Number(date[2]);
                                let hour = Number(tt[0]), mm = Number(tt[1]), sec = Number(tt[2].split('.')[0]);
                                let mil = tt[2].split('.').length != 1 ? tt[2].split('.')[1].split('Z')[0] : 0;
                                let utc = new Date(Date.UTC(year, mon - 1, dd, hour, mm, sec, mil));
                                let gte_time = moment(utc).add(-1 * within_time, 'm').toISOString();
                                let lte_time = moment(utc).add(within_time, 'm').toISOString();

                                let agg_query = [{
                                    $lookup: {
                                        from: collectionName2,
                                        pipeline: [
                                            {
                                                $match: {
                                                    location: {
                                                        $geoWithin: {
                                                            $centerSphere: [[Number(lng), Number(lat)], radius_dis]
                                                        }
                                                    },
                                                    time: {$gte: gte_time, $lte: lte_time}
                                                }
                                            }
                                        ],
                                        as: "join_result"
                                    }
                                }, {
                                    $match: {time: time, "join_result": {$ne: []}}
                                }, {
                                    $project: {"_id": 0, "join_result._id": 0}
                                }];
                                callback(null, lng, lat, time, extra, agg_query);
                            },
                            function (lng, lat, time, extra, agg_query, callback) {
                                db.collection(collectionName1).aggregate(agg_query, function (err, result) {
                                    let split_joinResult = "[]";
                                    let agg_join_count = 0;
                                    async.waterfall([
                                            function (callback) {
                                                result = JSON.stringify(result);
                                                callback(null, result);
                                            },
                                            function (result, callback) {
                                                let isExist = result.indexOf('[]');
                                                callback(null, isExist, result);
                                            },
                                            function (isExist, result, callback) {
                                                if (isExist == -1) {
                                                    split_joinResult = (result.split('join_result\":')[1]).slice(0, -2);
                                                    callback(null, isExist, (result.split('join_result\":')[1]).slice(0, -2));
                                                }
                                                else
                                                    callback(null, isExist, "[]");
                                            },
                                            function (isExist, result, callback) {
                                                if (isExist == -1) {
                                                    agg_join_count = result.split('},{').length;
                                                    callback(null, result.split('},{').length, result);
                                                }
                                                else
                                                    callback(null, 0, result);
                                            },
                                            function (agg_count, result, callback) {
                                                if (agg_count != 0) {
                                                    join_result[join_count] = lng + " " + lat + " " + time + " " + extra + " " + split_joinResult;
                                                    join_count++;
                                                }
                                                callback(null, agg_count, join_result);
                                            },
                                            function (agg_count, join_result, callback) {
                                                total = total + agg_count;
                                                callback(null, total, join_result);
                                            }
                                        ],
                                        function (err, total, join_result) {
                                            callback(null, lng, lat, time, extra, total, join_result);
                                        });
                                });
                            },
                            function (lng, lat, time, extra, total, join_result, callback) {
                                callback(null, total, join_result);
                            }],
                        function (err, total, result) {
                            if (err) console.log(err);
                            count = count + 1;
                            if (count == len) {

                                let i, j;
                                for (i = 0; i < join_result.length; i++) {
                                    let result1 = result[i].split('[')[0];
                                    let result2 = "[" + result[i].split(' [')[1];
                                    result2 = result2.split('},{');
                                    for (j = 0; j < result2.length; j++) {
                                        console.log(result1, result2[j]);
                                    }
                                }
                                if (i == join_result.length && j == result2.length) {
                                    console.timeEnd('startTime')
                                }

                                console.log(total);
                                res.render('lookupResult', {
                                    mod: 'lookup',
                                    title: 'Join Result - lookup ver.',
                                    within_time: within_time,
                                    within_distance: within_distance,
                                    result_count: join_result.length,
                                    result: result
                                });
                            }
                        }
                    );
                });
            });
        })
        .catch(function (err) {
            console.log("mongodb err ", err.code);
            return;
        });
};

module.exports = router;
