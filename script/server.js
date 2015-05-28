var http = require('http'),
    fs = require('fs'),
    url = require('url');
var MongoClient = require('mongodb').MongoClient;

var id = 0;
var id_match = 0;

var scoreHome = 0;
var scoreAway = 0;

var listing = {
	"games":{
		"rows":[{
		}]
	}
}

var LineByLineReader = require('line-by-line');
var path = "C:/Users/Giuseppe/Desktop/basketProjectBD/basketProjectBD/datasetNBA/";
var nameSeason = '06-07-PO';
var lr = new LineByLineReader(path+);

lr.on('error', function (err) {
    // 'err' contains error object
});

lr.on('line', function (line) {
	var content = {
			"id": "",
			"type": "",
			"date": "",
			"lineNumber": "",
			"id_match": "",
			"home": "",
			"away": "",
			"timeRemaining": "",
			"entry": "",
			"scoreHome": "",
			"scoreAway": ""
		};
	var row = line.split("\t");
	
	var entry;
	if (row[1] == 1)
		id_match++;
	content.id = id;
	if (line.indexOf("[") == -1)
		content.type = "gd";
	else {
		entry = row[3].split("]");
		if (entry[0].length == 4)
			content.type = "event";
		else content.type = "point";
	}
	content.date = line.substring(0,8);
	content.lineNumber = row[1];
	content.id_match = id_match;
	content.home = line.substring(8,11);
 	content.away = line.substring(11,14);
 	content.timeRemaining = row[2];
 	content.entry = row[3];
 	content.scoreHome = scoreHome;
 	content.scoreAway = scoreAway;
 	try {

 	entry = row[3].split("]");

	var res = entry[0].split(" ");

 	var points;

 	try {
		if (res[0].substring(1, res[0].length) == content.home) {
			points = res[1].split("-");
			scoreHome = points[0];
			content.scoreHome = scoreHome;

		}
		else if (res[0].substring(1, res[0].length) == content.away) {
			points = res[1].split("-");
			scoreAway = points[0];
			content.scoreAway = scoreAway;	
		}
	}
	catch(err) {
		content.scoreHome = scoreHome;
		content.scoreAway = scoreAway;
	}

 	
 	id++;

 	listing.games.rows.push(content);
	}
	catch(err) {
		console.log("missed row");
	}
});

lr.on('end', function () {
	MongoClient.connect("mongodb://localhost:27017/NBA", function(err, db) {
  		if(!err) {
    		console.log("We are connected");
  		}
		if(err) {
			console.log("non funziona");
		}

		var collection = db.collection(nameSeason);
		collection.insert(listing);
		console.log("finite");
	});
});