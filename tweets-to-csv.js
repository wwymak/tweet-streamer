const fs = require('fs');
const csvWriter = require('csv-write-stream');
const writer = csvWriter({ headers: ["tweet_id", "tweet_text", "timestamp", "user_id", "hashtags","url"]});
const MongoClient = require('mongodb').MongoClient;
const mongoURL = 'mongodb://localhost:27017/ukElectionTweets';
const commandLineArgs = require('command-line-args');
const optionDefinitions = [
  { name: 'startTimestamp', type: Number},
  { name: 'endTimestamp', type: Number}
];
const options = commandLineArgs(optionDefinitions);

let currStart = new Date(options.startTimestamp);
let fileName = `${currStart.getFullYear()}-${currStart.getMonth() + 1}-${currStart.getDate()}`;

// console.log(options.startTimestamp, new Date(options.startTimestamp))
writer.pipe(fs.createWriteStream(`/Volumes/SDExternal2/datasets/uk-election-tweets/${fileName}.csv`));

MongoClient.connect(mongoURL, function(err, db) {
    if(err) {
        console.log(err)
    } else {
        let stream = db.collection('tweets').find({timestamp: {$lte: options.endTimestamp, $gte: options.startTimestamp}}).stream();
        stream.on('data', function(data) {
            let urls = "";
            let hashtags = "";
            if(data.entities.urls.length > 0) {
                let urlArr = data.entities.urls.map(d => d.expanded_url).toString();
                urls = urlArr.toString();
            }
            if(data.entities.hashtags.length > 0) {
                let hashtagsArr = data.entities.hashtags.map(d => d.text);
                hashtags = hashtagsArr.toString();
            }
            let tweet = [data['id_str'], data.text, data.created_at, data.user.id_str,hashtags, urls];
            writer.write(tweet);
        });
        stream.on('end', () => {
            writer.end();
            db.close();
        })
    }
});
