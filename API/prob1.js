const csv = require('csv-parser');
const fs = require('fs');

var countObj = {}

module.exports = function (req, res) {
    const filePath = 'matches.csv';
    const selectedYears = ['2016', '2017'];
    const teamsByYear = {};


    fs.createReadStream(filePath)
    .pipe(csv())
    .on('data', (row) => {
        
        const year = row.SEASON;
        const tossWinnerTeam = row.TOSS_WINNER;
        
        if (selectedYears.includes(year) && row.TOSS_DECISION === 'field') {
            
            if (!teamsByYear[year]) {
                teamsByYear[year] = {};
            }
            
            if (!teamsByYear[year][tossWinnerTeam]) {
                teamsByYear[year][tossWinnerTeam] = 1;
            } else {
                teamsByYear[year][tossWinnerTeam]++;
            }

        }
    })
    .on('end', () => {
        selectedYears.forEach((year) => {
            const teamCounts = Object.entries(teamsByYear[year] || {});
            teamCounts.sort((a, b) => a[0].localeCompare(b[0]))
            countObj[year] = {...teamCounts.slice(0, 4)};
        });

        res.json(countObj);
    });
}