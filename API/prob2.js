const csv = require('csv-parser');
const fs = require('fs');

var matchObj={}
var finalObj={}
var nfinalObj={}
var batsman_runs=0;
var total_runs=0;
var temp_4=0;
var temp_6=0;

module.exports=function(req, res){
    const deliveriesFilePath = "deliveries.csv";
    const matchesFilePath = "matches.csv";

    let matches = fs.createReadStream(matchesFilePath).pipe(csv());
    let deliveries = fs.createReadStream(deliveriesFilePath).pipe(csv());

    matches.setMaxListeners(5000);
    deliveries.setMaxListeners(5000);
    
    const teamStats = {};
    
    matches.on('data', (match) => {
        deliveries.on('data', (delivery) => {
            if (match.MATCH_ID === delivery.MATCH_ID) {
                const season = match.SEASON;
                const battingTeam = delivery.BATTING_TEAM;
                const batsmanRuns = parseInt(delivery.BATSMAN_RUNS, 10);
                const extras = parseInt(delivery.EXTRA_RUNS, 10);

                if (!teamStats[season]) {
                    teamStats[season] = {};
                }

                if (!teamStats[season][battingTeam]) {
                    teamStats[season][battingTeam] = {
                        count4: 0,
                        count6: 0,
                        "total runs": 0,
                    };
                }

                teamStats[season][battingTeam]["total runs"] += batsmanRuns + extras;

                if (batsmanRuns === 4) {
                    teamStats[season][battingTeam].count4 += 1;
                } else if (batsmanRuns === 6) {
                    teamStats[season][battingTeam].count6 += 1;
                }
            }
        })
    });

    deliveries.on('end', () => {

        res.json(teamStats)
    })
}