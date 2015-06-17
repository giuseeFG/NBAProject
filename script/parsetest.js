var http = require('http'),
    fs = require('fs'),
    url = require('url'),
    MongoClient = require('mongodb').MongoClient,
    LineByLineReader = require('line-by-line'),
//C:/Users/Giuseppe/Desktop/basketProjectBD/basketProjectBD/datasetNBA/2011_2012.txt
//  /Volumes/MacbookHD/Documenti/MYSTUFF/RM3/2nd/BigData/dataset/datasetNBA/NBADATASET.txt
    lrPlayers = new LineByLineReader('/Volumes/MacbookHD/Documenti/MYSTUFF/RM3/2nd/BigData/dataset/datasetNBA/players2012.txt');

lrPlayers.on('error', function(err) {
    // 'err' contains error object
});

var players = [];
lrPlayers.on('line', function(line) {
	var row = line.split('\t');
	var player = {};
	player.name = row[1];
	player.trueName = row[2];
	player.team = row[4];
	players.push(player);
});



lrPlayers.on('end', function() {
    console.log("finish all players");
    parse(players);

});




function parse(players) {

    lr = new LineByLineReader('/Volumes/MacbookHD/Documenti/MYSTUFF/RM3/2nd/BigData/dataset/datasetNBA/2011-2012.txt');

lr.on('error', function(err) {
    // 'err' contains error object
});


lr.on('line', function(line) {

    var row = line.split("\t");
    var rep = row[3];
    try {
	    var res = rep.split("] ");
	    var team = res[0].substring(1,4);
	    var tmp = res[1].split(" ");
	    var name = '';
	    if(tmp[0].indexOf(".") !== -1) {
	    	name = tmp[0] + tmp[1];
	    }
	    else {
	    	name = tmp[0];
	    }
        var a  = false;
        for(player in players) {
            if((name === players[player].name) &&(team === players[player].team)){
                
            }
        }
    }

    catch (err) {
    	console.log(err);
    }
    
});
lr.on('end', function() {
    // saving into MongoDB the last match read.
    console.log("finish all");
});


}