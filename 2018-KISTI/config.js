var path = require('path');

const interfaces = require('os').networkInterfaces();
const address = Object.keys(interfaces)
    .reduce((results, name) => results.concat(interfaces[name]), [])
    .filter((iface) => iface.family === 'IPv4' && !iface.internal)
    .map((iface) => iface.address);

module.exports = {
    'DB_USER': 'KISTI',
    'DB_PWD': 'KISTI',
    'min_lng': 126.311,
    'min_lat': 35.217,
    'min_time': "2017-06-03T09:10:00",
    'xsize': 0.001,
    'ysize': 0.001,
    'tsize': '10m',
    'smax': 0.0001,
    'tmax': '60s',
    'partition': 'Partition',
    'port_number': 3000,
    'ip': address,
    'latestUpdate' : 20181212,
    'mongodb_path' : 'C:/Program Files/MongoDB 4.0.2/bin',
    'mongodb_url': 'mongodb://localhost:27017/',
    'result_path': path.join(process.cwd(), 'public/result')
}