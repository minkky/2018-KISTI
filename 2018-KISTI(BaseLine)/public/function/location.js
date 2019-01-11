var getLAT = function(lat1, lat2, cond){
  minlat = lat1 == '' ? 0 : lat1;
  maxlat = lat2 == '' ? 0 : lat2;
  tmp = minlat;

  minlat = minlat <= maxlat ? minlat : maxlat;
  maxlat = maxlat >= tmp ? maxlat : tmp;
  if(cond == 'MIN')
    return parseFloat(minlat);
  else if(cond == 'MAX')
    return parseFloat(maxlat);
};

var getLNG = function(lng1, lng2, cond){
  minlng = lng1 == '' ? 0 : lng1;
  maxlng = lng2 == '' ? 0 : lng2;
  tmp = minlng;

  minlng = minlng <= maxlng ? minlng : maxlng;
  maxlng = maxlng >= tmp ? maxlng : tmp;
  if(cond == 'MIN')
    return parseFloat(minlng);
  else if(cond == 'MAX')
    return parseFloat(maxlng);
};

exports.getLAT = getLAT;
exports.getLNG = getLNG;
