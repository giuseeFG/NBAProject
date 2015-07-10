var http = require('http'),
    fs = require('fs'),
    url = require('url'),
    LineByLineReader = require('line-by-line'),
    lrPlayers = new LineByLineReader('C:/Users/Giuseppe/Desktop/basketProjectBD/basketProjectBD/datasetNBA/players20112012.txt');

lrPlayers.on('error', function(err) {
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

var out = fs.createWriteStream("C:/Users/Giuseppe/Desktop/basketProjectBD/basketProjectBD/datasetNBA/DATASET_CLEANED/2011-2012.txt");
var errori = 0;
var scritti = 0;
var elseVal = 0;
function parse(players) {
    lr = new LineByLineReader('C:/Users/Giuseppe/Desktop/basketProjectBD/basketProjectBD/datasetNBA/2011-2012.txt');
    lr.on('error', function(err) {
    });
    lr.on('line', function(line) {
        var splitted = line.split("\t");
        var eventDetail = splitted[3];
        try {
            var res = eventDetail.split("] ");
            var team = res[0].substring(1, 4);
            var tmp = res[1].split(" ");
            var name = '';
            var trovato = false;
            if (tmp[0].indexOf(".") !== -1) {
                name = tmp[0] + tmp[1];
            } else {
                name = tmp[0];
            }
            for (player in players) {
                if ((name === players[player].name) && (team === players[player].team)) {
                    var lineNew = line.replace(name, players[player].trueName.replace(" ",""));
                    trovato = true;
                    console.log("yeah");
                    try {
                       out.write(lineNew + '\n');
                       scritti++;
                    }
                    catch(err) {
                        }
                }
            }
            if (!trovato) {
                out.write(line + '\n');
            }
        } catch (err) {            
            console.log("ERR " + err);
            out.write(line + '\n');
            errori++;
        }
    });
    lr.on('end', function() {
        console.log("finish all");
        console.log("errori " + errori);
        console.log("scritti " + scritti);
        console.log("elseVal " + elseVal);
    });
}