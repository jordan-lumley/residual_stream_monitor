const fs = require('fs'),
    csv = require('fast-csv'),
    path = require('path'),
    ObjectsToCsv = require('objects-to-csv');

exports.appendToCsv = async (csvFile, data) => {
    return new Promise(async (resolve, reject) => {
        try {
            if (path.extname(csvFile) === ".csv") {
                var csv = new ObjectsToCsv(data);
                await csv.toDisk(csvFile, { append: true });
            } else {
                reject(new Error('file not a valid csv'));
            }
        }
        catch (err) {
            reject(err);
        }
    });
};