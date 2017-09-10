const commandLineArgs = require('command-line-args')
const optionDefinitions = [
  { name: 'prefix', type: String}
];
const options = commandLineArgs(optionDefinitions);
const AWS = require('aws-sdk');
const MongoClient = require('mongodb').MongoClient;
const async = require('async');
const zlib = require('zlib');

const credentials = new AWS.SharedIniFileCredentials({profile: 'wwymakAdmin'});
const awsConfig = new AWS.Config({
    credentials: credentials,
    region: 'eu-west-1'
});

const s3 = new AWS.S3(awsConfig);

const bucketName = 'd4d-uk-ge-tweets';
const mongoURL = 'mongodb://localhost:27017/ukElectionTweets';

var dbInstance;

var params = {
    Bucket: bucketName, /* required */
    // Delimiter: 'STRING_VALUE',
    // EncodingType: url,
    // Marker: '',
    // MaxKeys: 0,
    Prefix: options.prefix//'2017/05/12/06/'
};

var files = [];
// s3.listObjects(params, function(err, data) {
//   if (err) console.log(err, err.stack); // an error occurred
//   else     console.log(data);           // successful response
// });

function getFileList(params, cb) {


    loadFilesFromS3(files, null, cb)


}

function listFilesCb(err, files) {
    console.log(err, files)
}

// loadFilesFromS3(files, undefined, listFilesCb)

// // function to call over and over
function listFilesFromS3(files, marker, cb){
    // are we paging from a specific point?
    if (marker){
        params.Marker = marker;
    }

    s3.listObjects(params, function(err, data) {
        if (err) {

            cb(err);
        }

        else {

            // concat the list of files into our collection
            files = files.concat(data.Contents);
            // are we paging?
            if (data.IsTruncated) {
                let length = data.Contents.length;
                let marker = data.Contents[length-1].Key;
                // recursion!
                loadFilesFromS3(files, marker, cb);
            } else {
                console.log('files loaded')
                cb(undefined, files.map(d=> d.Key));
            }
        }
    });
}

function parseS3TweetFile(stringContents) {
    let c = '['.concat(stringContents);
    let data = c.slice(0, c.length -2).concat(']');
    let tweets;

    try {
        tweets = JSON.parse(data);
        tweets.forEach(d => {
            d.timestamp = parseInt(d.timestamp_ms);
        });
    } catch (e) {
        console.log(data, 'cant json parse data')
        tweets = [];
    }

    // console.log(tweets);
    return tweets;
}

// function getFileFromS3(db, fileName, cbFunc) {
//     let getParams = {
//         Bucket: bucketName, // your bucket name,
//         Key: fileName // path to the object you're looking for
//     }
//
//     s3.getObject(getParams, function(err, data) {
//         // Handle any error and exit
//         if (err){
//             cbFunc(null, []);
//         } else {
//             let objectData = data.Body.toString('utf-8'); // Use the encoding necessary
//             db.collection('tweets').insertMany(parseS3TweetFile(objectData)).then(res => {
//                 cbFunc(null, parseS3TweetFile(objectData));
//             });
//
//         }
//
//     });
// }

function getFileFromS3(db, fileName, cbFunc) {
    let getParams = {
        Bucket: bucketName, // your bucket name,
        Key: fileName // path to the object you're looking for
    }

    s3.getObject(getParams, function(err, data) {
        // Handle any error and exit
        if (err){
            console.log(err)
            cbFunc(null);
        } else {
            let buffer = new Buffer(data.Body);
            zlib.gunzip(buffer, function(err2, decoded) {
                if (err2 || !decoded) {
                    console.log(err2)
                    cbFunc(null);
                } else {
                    let objectData = parseS3TweetFile(decoded.toString());
                    db.collection('tweets').insertMany(objectData, { ordered: false }, (err3,res) => {
                        if(err3) {
                            console.log(JSON.stringify(err3))
                        }
                        cbFunc(null);
                    });

                }
            });
            // let objectData = data.Body.toString('utf-8'); // Use the encoding necessary
            // console.log(objectData)

        }

    });
}

async.waterfall([
    function connectToDB(cb) {

        MongoClient.connect(mongoURL, function(err, db) {
            if(err) {
                cb(err)
            } else {
                dbInstance = db;
                cb(null, db)
            }
        })
    },
    function(db, cb) {
        listFilesFromS3(files, undefined, (err, fileNameArr) => {
            if (err) {
                console.log(err)
                cb(err)
            } else {
                cb(null, db, fileNameArr)
            }
        });
    },
    function(db, fileNameArr, cb) {
        async.mapSeries(fileNameArr, (fileName, callback) => {
            getFileFromS3(db, fileName, callback)
        }, (err, res) => {
            if(err) {
                console.log(err)
                cb(null)
            } else {
                cb(null)
            }
        });
    }
], (err, res) => {
    if(err) {
        console.log(err)
    } else {
        console.log('done')
    }
})
