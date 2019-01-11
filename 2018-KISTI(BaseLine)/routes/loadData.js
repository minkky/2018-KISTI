let exec = require('child_process').exec;

module.exports = {
    load_data1 : function(collectionName){
        console.log("1 START : DB IMPORT");
        let cmd = "cd \"C:/Program Files/MongoDB/Server/3.6/bin\" && mongoimport --db KISTI --collection " + collectionName + " --drop --file \"C:/Users/minji/Desktop/VC/2018-KISTI/public/load/data_1.json";
        exec(cmd, function(err, stdout, stderr){
            if(err){
                console.log('child process exited with error code', err.code, stderr);
                return;
            }
            console.log(stdout + "\n 1 END : DB IMPORT");
        });
    },

    load_data2 : function(collectionName){
        console.log("2 START : DB IMPORT");
        let cmd = "cd \"C:/Program Files/MongoDB/Server/3.6/bin\" && mongoimport --db KISTI --collection " + collectionName + " --drop --file \"C:/Users/minji/Desktop/VC/2018-KISTI/public/load/data_2.json";

        exec(cmd, function(err, stdout, stderr){
            if(err){
                console.log('child process exited with error code', err.code, stderr);
                return;
            }
            console.log(stdout + "\n 2 END : DB IMPORT");
        });
    }
};
