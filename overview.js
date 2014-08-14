var http = require('http');
var JSON = require('JSON');

function putES(_path,data){
  var put_data = JSON.stringify(data);
  // console.log(put_data);

  var put_options = {
      host: _host,
      port: _port,
      path: _path,
      method: 'PUT',
      headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': put_data.length
      }
  };

  var put_req = http.request(put_options, function(res) {
      res.setEncoding('utf8');
      res.on('data', function(data) {
        console.log(data);
      });
  });

  put_req.write(put_data);
  put_req.end();
}


function postES (_sourcePath,_sourceQuery,_sourceDate,_sourceField,_targetPath,_getTargetDataFun) {
  var post_data = 
  '{ \
      "query": { \
        "filtered": { \
          "query": { \
            "query_string": { \
              "query": "'+_sourceQuery+'" \
            } \
          }, \
          "filter": { \
            "bool": { \
              "must": [ \
                { \
                  "range": { \
                    "'+_sourceDate+'": { \
                      "from": 1356969600000 \
                    } \
                  } \
                } \
              ] \
            } \
          } \
        } \
      }, \
      "aggs":{ \
        "unique_user":{ \
          "date_histogram": { \
            "field": "'+_sourceDate+'", \
            "interval": "1M" \
          }, \
          "aggs":{ \
            "id":{ \
              "terms":{ \
                "field": "'+_sourceField+'", \
                "size": 1000 \
              } \
            } \
          } \
        } \
      } \
  }'; 
  
  var post_options = {
      host: _host,
      port: _port,
      path: _sourcePath,
      method: 'POST',
      headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': post_data.length
      }
  };

  var post_req = http.request(post_options, function(res) {
      res.setEncoding('utf8');
      var responseString = '';

      res.on('data', function(data) {
        responseString += data;
      });

      res.on('end', function() {
        var resultObject = JSON.parse(responseString);
        // console.log(responseString);
        resultObject.aggregations.unique_user.buckets.forEach(function(entry) {
            putES(_targetPath + entry.key, _getTargetDataFun(entry));

        });
      });
  });

  post_req.write(post_data);
  post_req.end();
}

// ===================================
var _host = "sj1glm661.corp.adobe.com";
var _port = 9200;

// ===================================
var _sourcePath = '/udc/_search?pretty';
var _sourceQuery = '_type:ComponentUsageDataEntity';
var _sourceDate = 'collectDate';
var _sourceField = 'user';
var _targetPath = '/udc2/ComponentUsageDataEntityMonth/';
var _get_unique_user = function(entry) {
  var users = [];
  entry.id.buckets.forEach(function(user) {
    users.push(user.key);
  });
  var _d = {};
  _d.collectDate=entry.key;
  _d.number=entry.id.buckets.length;
  _d.users=users;
  return _d;
}
postES(_sourcePath,_sourceQuery,_sourceDate,_sourceField,_targetPath,_get_unique_user);

// ===================================
var _sourcePath = '/udc/_search?pretty';
var _sourceQuery = '_type:CCMAccountEntity';
var _sourceDate = 'collectDate';
var _sourceField = 'ldap';
var _targetPath = '/udc2/CCMAccountEntityMonth/';
postES(_sourcePath,_sourceQuery,_sourceDate,_sourceField,_targetPath,_get_unique_user);

// ===================================
var _sourcePath = '/subscription_email_trigger/_search?pretty';
var _sourceQuery = '*';
var _sourceDate = '@timestamp';
var _sourceField = 'userId';
var _targetPath = '/udc2/subscription_email_trigger_Month/';
var _get_unique_user = function(entry) {
  var users = [];
  entry.id.buckets.forEach(function(user) {
    users.push(user.key);
  });
  var _d = {};
  _d['@timestamp']=entry.key;
  _d['number']=entry.id.buckets.length;
  _d['users']=users;
  return _d;
}
postES(_sourcePath,_sourceQuery,_sourceDate,_sourceField,_targetPath,_get_unique_user);



function getES(_path,_data,_d,endFun){
  var put_options = {
      host: _host,
      port: _port,
      path: _path,
      method: 'GET',
      headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': _data.length
      }
  };
  var put_req = http.request(put_options, function(res) {
      res.setEncoding('utf8');
      var responseString = '';
      res.on('data', function(data) {
        responseString += data;
      });
      res.on('end', function(){
        endFun(responseString,_d);
      });
  });
  put_req.write(_data);
  put_req.end();
}

var end_Requests_Fun = function(responseString,_d){
  var resultObject = JSON.parse(responseString);
  // console.log(responseString);
  var cur = resultObject.aggregations.requests.buckets[0].doc_count;
  var pre = "0";
  if(resultObject.aggregations.requests.buckets.length>1)
    pre = resultObject.aggregations.requests.buckets[1].doc_count;
  var tip = cur > pre ? '' : '';
  _d['Requests'] = cur + ' ' + tip + ' ( previous month: ' + pre + ' ) ';

  putES('/udc2/Summary/'+_d._pos,_d);
};

var end_Unique_Customers_Fun = function(responseString,_d) {
  var resultObject = JSON.parse(responseString);
  var cur = resultObject.hits.hits[0]._source.number;
  var cur_time = resultObject.hits.hits[0]._source[_d._time];
  var pre = resultObject.hits.hits[1]._source.number;
  var tip = cur > pre ? '' : '';
  _d['collectDate'] = cur_time;
  _d['Unique_Customers'] = cur + ' ' + tip + ' ( previous month: ' + pre + ' ) ';


  var _sourcePath = '/'+_d._index+'/'+_d._type+'/_search?pretty';
  var _sourceData = 
  '{ \
    "query": {\
      "filtered": {\
        "query": {\
          "query_string": {\
            "query": "'+_d._query+'"\
          }\
        }\
      }\
    },\
    "aggs":{ \
      "requests":{ \
        "date_histogram": { \
          "field": "'+_d._time+'", \
          "interval": "1M", \
          "order" : { "_key" : "desc" } \
        } \
      } \
    } \
  }';
  getES(_sourcePath, _sourceData, _d, end_Requests_Fun);
};


var _d = {};
_d._index='udc';
_d._type='ComponentUsageDataEntity';
_d._query='*';
_d._type2='ComponentUsageDataEntityMonth';
_d._pos='001';
_d._time='collectDate';
_d['Service'] = 'EASE';
var _sourcePath = '/udc2/'+_d._type2+'/_search?pretty';
var _sourceData = '{"sort":[{"'+_d._time+'":{"order":"desc"}}]}';
getES(_sourcePath, _sourceData, _d, end_Unique_Customers_Fun);


var _d = {};
_d._index='udc';
_d._type='CCMAccountEntity';
_d._query='*';
_d._type2='CCMAccountEntityMonth';
_d._pos='002';
_d._time='collectDate';
_d['Service'] = 'Batch Account Provisioning';
var _sourcePath = '/udc2/'+_d._type2+'/_search?pretty';
var _sourceData = '{"sort":[{"'+_d._time+'":{"order":"desc"}}]}';
getES(_sourcePath, _sourceData, _d, end_Unique_Customers_Fun);


var _d = {};
_d._index='subscription_email_trigger';
_d._type='subscription_email_trigger';
_d._query='endpoint:/[a-zA-Z0-9._]+/';
_d._type2='subscription_email_trigger_Month';
_d._pos='004';
_d._time='@timestamp';
_d['Service'] = 'Email Trigger';
var _sourcePath = '/udc2/'+_d._type2+'/_search?pretty';
var _sourceData = '{"sort":[{"'+_d._time+'":{"order":"desc"}}]}';
getES(_sourcePath, _sourceData, _d, end_Unique_Customers_Fun);
