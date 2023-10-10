const csv = require('csv-parser');
const fs = require('fs');

module.exports = function (req, res) {
    const deliveriesFilePath = 'deliveries.csv';
    const matchesFilePath = 'matches.csv';

    const matches = fs.createReadStream(matchesFilePath).pipe(csv());
    const deliveries = fs.createReadStream(deliveriesFilePath).pipe(csv());

    matches.setMaxListeners(5000);
    deliveries.setMaxListeners(5000);
    const teamStats = {};

    matches.on('data', (match) => {
        deliveries.on('data', (delivery) => {
            if (match.MATCH_ID === delivery.MATCH_ID) {
                const season = match.SEASON;
                const battingTeam = delivery.BATTING_TEAM;
                const bowlingTeam = delivery.BOWLING_TEAM;
                const runs = parseInt(delivery.TOTAL_RUNS, 10);
                const overs = parseFloat(delivery.OVER);
                
                if (!teamStats[season]) {
                    teamStats[season] = {};
                }

                if (!teamStats[season][battingTeam]) {
                    teamStats[season][battingTeam] = {
                        runsScored: 0,
                        oversFaced: 0,
                        runsConceded: 0,
                        oversBowled: 0,
                    };
                }

                if (!teamStats[season][bowlingTeam]) {
                    teamStats[season][bowlingTeam] = {
                        runsScored: 0,
                        oversFaced: 0,
                        runsConceded: 0,
                        oversBowled: 0,
                    };
                }
                
                teamStats[season][battingTeam]['runsScored'] += runs;
                teamStats[season][battingTeam]['oversFaced'] += overs;

                teamStats[season][bowlingTeam]['runsConceded'] += runs;
                teamStats[season][bowlingTeam]['oversBowled'] += overs;

            }
        });
    });

    deliveries.on('end', () => {
        const netRunRateStats = {};
        for (const season in teamStats) {
            netRunRateStats[season] = {
                netRunRate: -Infinity,
                team: '',
            };
            
            for (const team in teamStats[season]) {
                const runsScored = teamStats[season][team].runsScored;
                const oversFaced = teamStats[season][team].oversFaced;
                const runsConceded = teamStats[season][team].runsConceded;
                const oversBowled = teamStats[season][team].oversBowled;

                if (oversFaced > 0 && oversBowled > 0) {
                    const netRunRate = ((runsScored / oversFaced) - (runsConceded / oversBowled)).toFixed(2);

                    if (parseFloat(netRunRate) > netRunRateStats[season].netRunRate) {
                        netRunRateStats[season].netRunRate = parseFloat(netRunRate);
                        netRunRateStats[season].team = team;
                    }
                }
            }
        }

        res.json(netRunRateStats);
    });
};
