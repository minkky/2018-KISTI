let express = require('express');
let value = require('../public/function/value');
let router = express.Router();
let appendPartitionDone = false;

/* GET users listing. */
router.get('/', function(req, res, next) {
    res.render('appendPartition', { title: 'appendPartition' });
});

module.exports = router;
