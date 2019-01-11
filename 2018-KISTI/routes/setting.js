var express = require('express');
var router = express.Router();
var value = require('../public/function/value');
var partition = require('../public/function/partition');
let config = require('../config');
//const {enable, destroy} = require('splash-screen');


router.post('/btnClicked', function (req, res, next) {
    //enable('tailing');
    let type = req.body.submit;

    if (type == "search") {
        res.redirect("/search");
    }
    else {
        console.log(type, "start !");
        let xsize = (req.body.partition_distance_interval === "") ? config.xsize : parseFloat(req.body.partition_distance_interval);
        let ysize = (req.body.partition_distance_interval === "") ? config.ysize : parseFloat(req.body.partition_distance_interval);
        let tsize = (req.body.partition_time_interval === "") ? config.tsize : req.body.partition_time_interval;
        let smax = (req.body.near_smax === "") ? config.smax : req.body.near_smax;
        let tmax = (req.body.near_tmax === "") ? config.tmax : req.body.near_tmax;
        let minlng = (req.body.min_lng === "") ? config.min_lng : req.body.min_lng;
        let minlat = (req.body.min_lat === "") ? config.min_lat : req.body.min_lat;
        let mintime = (req.body.min_time === "") ? config.min_time : req.body.min_time;

        value.setXSize(xsize);
        value.setYSize(ysize);
        value.setSMax(smax);
        value.setTMax(tmax);
        value.setMinLNG(minlng);
        value.setMinLAT(minlat);
        let time = new Date(mintime);
        time.setHours(time.getHours() + 9);
        value.setMinTIME(mintime);
        config.min_lng = minlng;
        config.min_lat = minlat;
        config.min_time = mintime;
        value.setPartitionSettingDone(false);

        value.transformSec(tsize)
            .then(function (s) {
                return new Promise(resolve => {
                    value.setTSize(s);
                    resolve("all set");
                })
                    .then(function (result) {
                        try {
                            partition.createAndFillPartition()
                                .then(function (result) {
                                    if (result) {
                                        console.log("> Partition setting is all done");
                                        value.setPartitionSettingDone(true);
                                        console.log("partition setting done : " + value.getPartitionSettingDone() + " append left.");
                                        res.redirect("/appendPartition");
                                    }
                                });
                        }
                        catch (e) {
                            console.log(e);
                        }
                    });
            });

    }
});

module.exports = router;