'use strict';
//trying different streaming module since the 'twitter' seem to ahve problmes with the conncection hanging up
var Twitter = require('node-tweet-stream');// require('twitter');

var credentials = require('./config/twitterConf');

var client = new Twitter({
    consumer_key: process.env.twitConsumerKey || credentials.consumer.key,
    consumer_secret: process.env.twitConsumerSecret || credentials.consumer.secret,
    token: process.env.twitAcessToken ||credentials.access_token.key,
    token_secret: process.env.twitAcessTokenSecret || credentials.access_token.secret
});
var config = require('./config/awsConf');
// var Twit = require('twit');
var util = require('util');
var logger = require('./util/logger');

function twitterStreamProducer(firehose) {
  var waitBetweenPutRecordsCallsInMilliseconds = config.waitBetweenPutRecordsCallsInMilliseconds;
  var log = logger().getLogger('producer');

  // Creates a new kinesis stream if one doesn't exist.
  function _createStreamIfNotCreated(callback) {
    firehose.describeDeliveryStream({DeliveryStreamName: config.firehose.DeliveryStreamName}, function(err, data) {
      if (err) {
        firehose.createDeliveryStream(config.firehose, function(err, data) {
          if (err) {
            // ResourceInUseException is returned when the stream is already created.
            if (err.code !== 'ResourceInUseException') {
              console.log(err);
              callback(err);
              return;
            }
            else {
              var msg = util.format('%s stream is already created! Re-using it.', config.firehose.DeliveryStreamName);
              console.log(msg);
              log.info(msg);
            }
          }
          else {
            var msg = util.format('%s stream does not exist. Created a new stream with that name.', config.firehose.DeliveryStreamName);
            console.log(msg);
            log.info(msg);
          }
          // Poll to make sure stream is in ACTIVE state before start pushing data.
          _waitForStreamToBecomeActive(callback);
        });
      }
      else {
        var msg = util.format('%s stream is already created! Re-using it.', config.firehose.DeliveryStreamName);
        console.log(msg);
        log.info(msg);
      }

      // Poll to make sure stream is in ACTIVE state before start pushing data.
      _waitForStreamToBecomeActive(callback);
    });


  }

  // Checks current status of the stream.
  function _waitForStreamToBecomeActive(callback) {
    firehose.describeDeliveryStream({DeliveryStreamName: config.firehose.DeliveryStreamName}, function(err, data) {
      if (!err) {
        if (data.DeliveryStreamDescription.DeliveryStreamStatus === 'ACTIVE') {
          console.info('Current status of the stream is ACTIVE.');
          callback(null);
        }
        else {
          var msg = util.format('Current status of the stream is %s.', data.DeliveryStreamDescription.DeliveryStreamStatus);
          console.log(msg);
          setTimeout(function() {
            _waitForStreamToBecomeActive(callback);
          }, 1000 * config.waitBetweenDescribeCallsInSeconds);
        }
      }
    });
  }

  /**
   *
   * @param tweet one item in  tweets.statuses
   * @param storeTweetText {Boolean} whether you want to store the text of the tweets as well
   * @returns {{id: (*|String|number|string)}}
   */
  function tweetObj(tweet, storeTweetText = false){
      var tweetModel =  {
          id: tweet.id_str,
          lang: tweet.lang,
          created_at: tweet.created_at,
          screen_name: tweet.user.screen_name,
          coordinates: tweet.coordinates,
          place: tweet.place,
          user_location : tweet.user.location
      }

      if(storeTweetText == true){
          tweetModel.text =  tweet.text;
      }
      return tweetModel
  }


  function _sendToFirehose() {
    var records = [];
    var record = {};
    var recordParams = {};
    client.on('tweet', function(tweet) {
    //   var tweetParsed = tweetObj(tweet, true);
        // if (tweetParsed.user_location){
        //     if (tweetParsed.user_location !== null){
            //   console.log(JSON.stringify(tweet));
              recordParams = {
                  DeliveryStreamName: config.firehose.DeliveryStreamName,
                  Record: {
                    Data: JSON.stringify(tweet)+',\n'
                  }
              };
              firehose.putRecord(recordParams, function(err, data) {
                if (err) {
                  log.error(err);
                }
              });

        //   }
        // }
    });

    client.on('error', function (err) {
      console.log('error!!!', err);
    });

    client.track('ge2017,generalelection,labour,conservatives,tories,ukip,libdems,greens,snp,brexit')

  }


  return {
    run: function() {
      log.info(util.format('Configured wait between consecutive PutRecords call in milliseconds: %d',
          waitBetweenPutRecordsCallsInMilliseconds));
      _createStreamIfNotCreated(function(err) {
        if (err) {
          log.error(util.format('Error creating stream: %s', err));
          return;
        }

        _sendToFirehose();
      });
    }
  };
}



module.exports = twitterStreamProducer;
