var http = require('http'),
    fs = require('fs'),
    url = require('url');
var id = 0;
var id_match = 0;

var scoreHome = 0;
var scoreAway = 0;

var game = {
	"game":{
		"rows":[{
		}]
	}
}

var LineByLineReader = require('line-by-line'),
    lr = new LineByLineReader('/Users/tiziano/Desktop/progettoultimoBIGDATA/datasetNBA/2006-2007PO.txt');

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

 	game.game.rows.push(content);

});

lr.on('end', function () {
	//cancellare il primo elemento del JSONArray.
	console.log(JSON.stringify(game));


});