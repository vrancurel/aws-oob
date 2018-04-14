var AWS = require('aws-sdk');
var sns = new AWS.SNS();
var sqs = new AWS.SQS({apiVersion: '2012-11-05'});
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

function allowTopicToWriteToQueue(queueUrl, queueArn, topicArn, cb) {
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
        return cb(result);
    });
}

//listQueues();

const QUEUE_NAME = 'foo_queue';
const TOPIC_NAME = 'foo_topic';

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
        allowTopicToWriteToQueue(queueUrl, queueArn, topicArn, callback);
    }
], (err, result) => {
    if (err) {
        console.log(err);
        process.exit(1);
    }
    console.log(result);
});
