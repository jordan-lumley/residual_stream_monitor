const fs = require('fs'),
    csv = require('fast-csv'),
    path = require('path');

exports.parseCsv = async (csvFile, csvParsingOpts) => {
    return new Promise((resolve, reject) => {
        var rows = [];

        var isHeaderDefined = false;
        var definingObject = {};

        try {
            if (path.extname(csvFile) === ".csv") {
                fs.exists(csvFile, (exists) => {
                    if (!exists) reject(new Error("file does not exist"));

                    fs.createReadStream(csvFile)
                        .pipe(csv.parse(csvParsingOpts))
                        .on('error', (error) => {
                            reject(error);
                        })
                        .on('data', (row) => {
                            var keys = Object.keys(definingObject);

                            if (!isHeaderDefined) {
                                for (let i = 0; i < row.length; i++) {
                                    const element = row[i];
                                    definingObject[element.toLowerCase()] = i;
                                }

                                isHeaderDefined = true;
                            } else {
                                var obj = {};

                                for (let i = 0; i < keys.length; i++) {
                                    obj[keys[i]] = row[i];
                                }

                                rows.push(obj);
                            }
                        })
                        .on('end', rowCount => {
                            var obj = {
                                count: rowCount,
                                data: rows,
                                fileName: path.basename(csvFile)
                            };

                            resolve(obj);
                        });
                })

            } else {
                resolve();
            }
        }
        catch (err) {
            reject(err);
        }
    });
};