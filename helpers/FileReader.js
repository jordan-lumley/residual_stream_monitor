const fs = require('fs');

exports.getFiles = (dir) => {
    return new Promise((resolve, reject) => {
        let files = [];
        try {
            fs.readdir(dir, (error, list) => {
                if (error) {
                    reject(error);
                }

                for (let i = 0; i < list.length; i++) {
                    const file = list[i];

                    if (file) {
                        var obj = {
                            path: `${dir}/${file}`,
                            residualType: "",
                            merchant: ""
                        }
                        files.push(obj);
                    }
                }

                resolve(files);
            });
        } catch (err) {
            reject(err);
        }
    })
};