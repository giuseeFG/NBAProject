var http = require('http'),
    fs = require('fs'),
    url = require('url');
var MongoClient = require('mongodb').MongoClient;

var id = 0;

var scoreHome = 0;
var scoreAway = 0;

var listing = [{}]

var LineByLineReader = require('line-by-line'),
    lr = new LineByLineReader('/Users/tiziano/Desktop/NBAProject/datasetNBA/2007-2008.txt');

lr.on('error', function(err) {
    // 'err' contains error object
});

var first = false;
var match;
lr.on('line', function(line) {
    var row = line.split("\t");
    if (row[1] == 1) {
    	if (first)
    		listing.push(match);
        id++;
        first = true;
        match = {
            "id_match": "",
            "date": "",
            "home": "",
            "away": "",
            "report": []
        };

        match.id_match = id;
        match.date = line.substring(0, 8);
        match.home = line.substring(8, 11);
        match.away = line.substring(11, 14);

    }

    var reportMatch = {
        "type": "",
        "idLineEvent": "",
        "home": "",
        "away": "",
        "timeRemaining": "",
        "entry": "",
        "scoreHome": "",
        "scoreAway": ""
    };


    var entry;

    if (line.indexOf("[") == -1)
        reportMatch.type = "gameDescription";
    else {
        entry = row[3].split("]");
        if (entry[0].length == 4)
            reportMatch.type = "generalEvent";
        else reportMatch.type = "point";
    }


    reportMatch.idLineEvent = row[1];
    reportMatch.home = line.substring(8, 11);
    reportMatch.away = line.substring(11, 14);
    reportMatch.timeRemaining = row[2];
    reportMatch.entry = row[3];
    reportMatch.scoreHome = scoreHome;
    reportMatch.scoreAway = scoreAway;
    match.report.push(reportMatch);


    try {

        entry = row[3].split("]");

        var res = entry[0].split(" ");

        var points;

        try {
            if (res[0].substring(1, res[0].length) == reportMatch.home) {
                points = res[1].split("-");
                scoreHome = points[0];
                reportMatch.scoreHome = scoreHome;

            } else if (res[0].substring(1, res[0].length) == reportMatch.away) {
                points = res[1].split("-");
                scoreAway = points[0];
                reportMatch.scoreAway = scoreAway;
            }
        } catch (err) {
            reportMatch.scoreHome = scoreHome;
            reportMatch.scoreAway = scoreAway;
        }

	} 


	catch (err) {
        console.log("missed row");
    }
});

lr.on('end', function() {

	fs.writeFile("./output.json", JSON.stringify(listing, null, 4), function(err){
		if(err)
			console.log(err)
		else
			console.log("salvato")
	})


    // MongoClient.connect("mongodb://localhost:27017/NBA", function(err, db) {
    //     if (!err) {
    //         console.log("We are connected");
    //     }
    //     if (err) {
    //         console.log("non funziona");
    //     }

    //     var collection = db.collection('1test03-06');
    //     collection.insert(listing);
    //     console.log("finite");
    // });
});