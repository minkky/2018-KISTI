let result = [];
let resultCount = 0;
let loc = [];
let time = [];
let extras = [];

let splitResult = function(result){
    let arr = result.split('},');
    return arr;
};

let splitLocation = function(docs){
    let tmp = docs.substring(1);
    tmp = (tmp.split('[')[1]).split(']')[0];
    return tmp;
}

let setLocation = function (docs) {
    for(i=0; i<docs.length; i++){
        loc[i] = splitLocation(docs[i]);
    }
}

let getLocation = function(){
    return loc;
};

let splitTime = function (docs) {
    let tmp = docs.split('time')[1].substring(2);
    tmp = tmp.split(',')[0];
    tmp = (tmp.replace('\"','')).replace('\"','');
    return tmp;
}

let setTime = function (docs) {
    for (i=0; i<docs.length; i++){
        time[i] = splitTime(docs[i]);
    }
}

let getTime = function () {
    return time;
};

let splitExtraInfo = function (docs) {
    let info = (docs.replace(' ','')).split('values')[1];
    info = (info.replace('{','')).replace('}','');
    info = info.substring(2).replace('}','').replace(']','');
    return info;
}

let setExtraInfo = function(docs){
    for(i =0; i<docs.length; i++){
        extras[i] = splitExtraInfo(docs[i]);
    }
}

let getExtraInfo = function () {
    return extras;
};

let modifyResult = function(result){
    m_result = [];
    for(i = 0; i<result.length; i++){
        json = result[i];
        if(i == 0){
            json = json + "}";
            json = json.substring(1);
        }
        else if(i == result.length -1){
            json = "{" + json;
            json = json.substring(0, json.length -1);
        }
        else
            json = "{" + json + "}";
        m_result[i] = json;
    }
    return m_result;
};

let setQuery = function(query){
    query_list = query;
    return query;
}

let setResults = function(query){
    //result = [];
    result = query;
    setResultCount(result.length);
}

let getResults = function(){
    if(getResultsCount() == 0) return undefined;
    return result;
}

let setResultCount = function(count){
    resultCount = count;
}

let getResultsCount = function(){
    return resultCount;
}

exports.splitLocation = splitLocation;
exports.splitTime = splitTime;
exports.splitExtraInfo = splitExtraInfo;

exports.setTime = setTime;
exports.setQuery = setQuery;
exports.setExtraInfo = setExtraInfo;
exports.setResults = setResults;
exports.setLocation = setLocation;
exports.splitResult = splitResult;

exports.getTime = getTime;
exports.getLocation = getLocation;
exports.getExtraInfo = getExtraInfo;
exports.getResults = getResults;
exports.getResultCount = getResultsCount;
exports.modifyResult = modifyResult;
