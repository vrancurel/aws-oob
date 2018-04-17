var AWS = require('aws-sdk');
var sns = new AWS.SNS();
var sqs = new AWS.SQS({apiVersion: '2012-11-05'});
var s3 = new AWS.S3();
var util = require('util');
var async = require('async');

AWS.config.update({region: process.env.AWS_REGION});

function listQueues() {
    var params = {};
    
    sqs.listQueues(params, function(err, data) {
        if (err) {
            console.log("Error", err);
        } else {
            console.log("Success", data.QueueUrls);
        }
    });
}

function createQueue(name, cb) {
    var params = {
        QueueName: name,
        Attributes: {
            'DelaySeconds': '60',
            'MessageRetentionPeriod': '86400'
        }
    };
    
    sqs.createQueue(params, function(err, data) {
        if (err) {
            return cb(err);
        }
        return cb(null, data.QueueUrl);
    });
}

function getQueueUrl(name, cb) {
    var params = {
        QueueName: name
    };

    sqs.getQueueUrl(params, function(err, data) {
        if (err) {
            return cb(err);
        }
        return cb(null, data.QueueUrl);
    });
}

function deleteQueue(name, cb) {
    var params = {
        QueueUrl: name,
    };
    
    sqs.deleteQueue(params, function(err, data) {
        if (err) {
            return cb(err);
        }
        return cb(null);
    });
}

function getQueueArn(url, cb) {
    sqs.getQueueAttributes({
        QueueUrl: url,
        AttributeNames: ["QueueArn"]
    }, (err, result) => {
        if (err) {
            return cb(err);
        }
        return cb(null, result.Attributes.QueueArn);
    });
}

function createTopic(name, cb) {
    sns.createTopic({
        'Name': name
    }, function (err, result) {
        if (err !== null) {
            return cb(err);
        }
        return cb(null, result.TopicArn);
    });
}

function subscribe(topicArn, queueArn, cb) {
    sns.subscribe({
        'TopicArn': topicArn,
        'Protocol': 'sqs',
        'Endpoint': queueArn
    }, (err, result) => {
        if (err) {
            return cb(err);
        }
        return cb(null, result);
    });
}

function allowTopicToWriteToQueue(queueUrl, topicArn, queueArn, cb) {
    var attributes = {
        "Version": "2008-10-17",
        "Id": queueArn + "/SQSDefaultPolicy",
        "Statement": [{
            "Sid": "Sid" + new Date().getTime(),
            "Effect": "Allow",
            "Principal": {
                "AWS": "*"
            },
            "Action": "SQS:SendMessage",
            "Resource": queueArn,
            "Condition": {
                "ArnEquals": {
                    "aws:SourceArn": topicArn
                }
            }
        }
                     ]};

    sqs.setQueueAttributes({
        QueueUrl: queueUrl,
        Attributes: {
            'Policy': JSON.stringify(attributes)
        }
    }, (err, result) => {
        if (err) {
            return cb(err);
        }
        return cb(null, result);
    });
}

function allowS3ToWriteToTopic(bucketName, topicArn, cb) {
    var attributes = {
        "Version": "2008-10-17",
        "Id": topicArn + "/SQSDefaultPolicy",
        "Statement": [{
            "Sid": "Sid" + new Date().getTime(),
            "Effect": "Allow",
            "Principal": {
                "AWS": "*"
            },
            "Action": "SNS:Publish",
            "Resource": topicArn,
            "Condition": {
                "ArnLike": {
                    "aws:SourceArn": `arn:aws:s3:*:*:${bucketName}`
                }
            }
        }
                     ]};

    sns.setTopicAttributes({
        TopicArn: topicArn,
        AttributeName: 'Policy',
        AttributeValue: JSON.stringify(attributes)
    }, (err, result) => {
        if (err) {
            return cb(err);
        }
        return cb(null, result);
    });
}

/** 
 * Register events
 * 
 * @param bucketName 
 * @param queueArn
 * @param topicArn
 * @param events an array of
 * [ s3:ReducedRedundancyLostObject | s3:ObjectCreated:* |
 * s3:ObjectCreated:Put | s3:ObjectCreated:Post |
 * s3:ObjectCreated:Copy | s3:ObjectCreated:CompleteMultipartUpload |
 * s3:ObjectRemoved:* | s3:ObjectRemoved:Delete |
 * s3:ObjectRemoved:DeleteMarkerCreated ]
 * @param cb
 * 
 * @return 
 */
function putBucketNotification(bucketName, queueArn, topicArn, events, cb) {
    var params = {
        Bucket: bucketName,
        NotificationConfiguration: {
            /*QueueConfiguration: {
                Events: events,
                Queue: queueArn
            },*/
            TopicConfiguration: {
                Events: events,
                Topic: topicArn
            }
        }
    };

    s3.putBucketNotification(params, (err, data) => {
        if (err) {
            return cb(err);
        }
        return cb(null, data)
    });
}

//listQueues();

const QUEUE_NAME = 'foo_queue';
const TOPIC_NAME = 'foo_topic';

if (process.argv.length != 3) {
    console.log('usage: index bucket');
    process.exit(1);
}

const BUCKET_NAME = process.argv[2];

console.log('BUCKET_NAME', BUCKET_NAME);

async.waterfall([
    (callback) => {
        createQueue(QUEUE_NAME, callback);
    },
    (queueUrl, callback) => {
        console.log('queueUrl', queueUrl);
        getQueueArn(queueUrl, (err, queueArn) => {
            if (err) {
                return callback(err);
            }
            return callback(null, queueUrl, queueArn);
        });
    },
    (queueUrl, queueArn, callback) => {
        console.log('queueArn', queueArn);
        createTopic(TOPIC_NAME, (err, topicArn) => {
            if (err) {
                return callback(err);
            }
            return callback(null, queueUrl, queueArn, topicArn);
        });
    },
    (queueUrl, queueArn, topicArn, callback) => {
        console.log('topicArn', topicArn);
        subscribe(topicArn, queueArn, (err, result) => {
            if (err) {
                return callback(err);
            }
            return callback(null, queueUrl, queueArn, topicArn, result);
        });
    },
    (queueUrl, queueArn, topicArn, subscribeResult, callback) => {
        console.log('subscribeResult', subscribeResult);
        allowTopicToWriteToQueue(queueUrl, topicArn, queueArn, (err, result) => {
            if (err) {
                return callback(err);
            }
            return callback(null, queueUrl, queueArn, topicArn, result);
        });
    },
    (queueUrl, queueArn, topicArn, allowResult, callback) => {
        console.log('allowResult1', allowResult);
        allowS3ToWriteToTopic(BUCKET_NAME, topicArn, (err, result) => {
            if (err) {
                return callback(err);
            }
            return callback(null, queueUrl, queueArn, topicArn, result);
        });
    },
    (queueUrl, queueArn, topicArn, allowResult, callback) => {
        console.log('allowResult2', allowResult);
        putBucketNotification(BUCKET_NAME, queueArn, topicArn, [
            's3:ObjectCreated:*'
        ], (err, data) => {
            if (err) {
                return callback(err);
            }
            console.log(data);
            return callback(null, queueUrl, queueArn);
        });
    }
], (err, result) => {
    if (err) {
        console.log(err);
        process.exit(1);
    }
    console.log(result);
});
