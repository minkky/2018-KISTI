let express = require('express');
let path = require('path');
let fs = require('fs');
let config = require('../config');
let router = express.Router();
let value = require('../public/function/value');
let joinPartition = require('../public/function/joinPartition');
let partitions = "Partition";


function setLocTimeInteval(cond1, cond2) {
    return new Promise(resolve => {
        value.getDB().collection(partitions).find({_id: "metadata"}).forEach(function (doc) {
            if (doc) {
                value.setXSize(doc.x_size);
                value.setYSize(doc.y_size);
                value.setTSize(doc.t_size);
                resolve("set done");
            }
        });
    });
}

function modifyJoinCollection(thingId) {
    let taxi_collection = thingId.split(",");
    for (let i = 0; i < taxi_collection.length; i++) {
        taxi_collection[i] = taxi_collection[i].replace(/(\s*)/g, "");
        taxi_collection[i] = taxi_collection[i].toUpperCase();
    }
    return taxi_collection;
}

router.post('/btnClicked', function (req, res, next) {
    let time = new Date(config.min_time);
    time.setHours(time.getHours() + 9);
    value.setMinTIME(time);
    value.setMinLNG(config.min_lng);
    value.setMinLAT(config.min_lat);

    let thingId = req.body.thingId == "" ? ["IK1001", "IK1008"] : modifyJoinCollection(req.body.thingId);
    let tx = ty = req.body.distance_interval;
    let tt = req.body.time_interval;
    value.setJoinCollection(thingId.sort());
    let resultPath = path.join(process.cwd(), "public/result");
    let ids = "";
    value.getJoinCollection().forEach(function (id) {
        ids += id + '_';
    });

    setLocTimeInteval(value.getXSize(), value.getTSize())
        .then(function () {
            value.setSearchLocInterval(tx);// value.setSMax(tx);
            value.setSearchTimeInterval(tt);// value.setTMax(tt);
            let fileName = path.join(resultPath, "result" + ids + "p" + value.getXSize() + "_p" + value.getTSize()  + "_c"+ value.getSearchTimeInterval() + "_c"+ value.getSearchLocInterval() + ".txt");
            let fileLink = "http://"+ config.ip+ ":"+ config.port_number + "/"+ "result/result" + ids + "p" + value.getXSize() + "_p" +  value.getTSize()  + "_c"+ value.getSearchTimeInterval() + "_c"+ value.getSearchLocInterval() + ".txt";
            fileExist(fileName)
                .then(function (result) {
                    joinPartition.appendFileWith(fileName, "")
                        .then(function (res) {
                            ;
                        });

                    joinPartition.findJoinCollections(fileName, process.hrtime());
                    res.render('searchResult', {
                        filelink : fileLink,
                        mod: 'base',
                        title: 'Join Result - base ver.',
                        within_time: value.getTMax(),
                        within_distance: value.getSMax(),
                        result_count: 0,
                        result: 0
                    });
                })

        });
});

function fileExist(fileName) {
    return new Promise(resolve => {
        fs.exists(fileName, function (exists) {
            if (exists) {
                fs.unlink(fileName, (err) => {
                    if (err) throw  err;
                    console.log(fileName + " removed");
                    resolve(fileName);
                });
            }
            else {
                console.log(fileName + " not exist");
                resolve(fileName);
            }
        });
    });
}

module.exports = router;
