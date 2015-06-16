var http = require('http'),
    fs = require('fs'),
    url = require('url');
var MongoClient = require('mongodb').MongoClient;

var id = 6545;

var scoreHome = 0;
var scoreAway = 0;

var listing = [{}]

var LineByLineReader = require('line-by-line'),
    lr = new LineByLineReader('C:/Users/Giuseppe/Desktop/basketProjectBD/basketProjectBD/datasetNBA/2011_2012.txt');

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
        // creo un "match" ogni volta che incontro un lineNumber == 1
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

    //se non c'è questo carattere: "["
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
        console.log("match " + match.id_match);
    } catch (err) {
        console.log("missed row " + err + " " + match.id_match);
    }
});

lr.on('end', function() {
    fs.writeFile("./2011_2012.json", JSON.stringify(listing, null, 4), function(err){
        if (err)
            console.log(err)
        else
            console.log("file salvato")
    })
});