const csv = require('csv-parser');
const fs = require('fs');

module.exports = function (req, res) {
	const deliveriesFilePath = 'deliveries.csv';
	const matchesFilePath = 'matches.csv';

	const matches = fs.createReadStream(matchesFilePath).pipe(csv());
	const deliveries = fs.createReadStream(deliveriesFilePath).pipe(csv());
	matches.setMaxListeners(5000);
    deliveries.setMaxListeners(5000);
	const bowlerStats = {};

	matches.on('data', (match) => {
		deliveries.on('data', (delivery) => {
			if (match.MATCH_ID === delivery.MATCH_ID) {
				const season = match.SEASON;
				const bowler = delivery.BOWLER;
				const wideRuns = parseInt(delivery.WIDE_RUNS, 10);
				const noBallRuns = parseInt(delivery.NOBALL_RUNS, 10);
				const totalRuns = parseInt(delivery.TOTAL_RUNS, 10);
				const isExtra = delivery.IS_EXTRA === '1'; // Check if the delivery is an extra

				if (!bowlerStats[season]) {
					bowlerStats[season] = {};
				}

				if (!bowlerStats[season][bowler]) {
					bowlerStats[season][bowler] = {
						"total runs": 0,
						balls: 0,
					};
				}

				// Exclude wide runs, no ball runs, and extras from total runs given by a bowler
				if (!isExtra) {
					bowlerStats[season][bowler]["total runs"] += totalRuns;
					bowlerStats[season][bowler].balls += 1;
				}
			}
		});
	});

	deliveries.on('end', () => {
		const economyStats = {};

		// Calculate economy for each bowler in each season
		for (const season in bowlerStats) {
			economyStats[season] = [];

			for (const bowler in bowlerStats[season]) {
				const runs = bowlerStats[season][bowler]['total runs'];
				const balls = bowlerStats[season][bowler].balls;
				const overs = balls / 6.0; // 1 over = 6 balls

				if (overs >= 10) {
					const economy = (runs / overs).toFixed(2); // Calculate economy rate with 2 decimal places
					economyStats[season].push([bowler, { 'total runs': runs, balls: balls, economy: parseFloat(economy) }]);
				}
			}

			// Sort bowlers by economy rate (lowest to highest)
			economyStats[season].sort((a, b) => a[1].economy - b[1].economy);

			// Take the top 10 bowlers
			economyStats[season] = economyStats[season].slice(0, 10);
		}

		res.json(economyStats);
	});
};
