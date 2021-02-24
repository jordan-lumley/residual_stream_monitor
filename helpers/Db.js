const pgp = require('pg-promise')();
const queries = require('./Queries');

const dbConfig = {
  host: '18.206.38.42',
  port: 5432,
  database: 'meridianstar',
  user: 'postgres',
  password: 'password',
};

const db = pgp(dbConfig);

exports.importToDb = async (csvData) => {
  return new Promise((resolve, reject) => {
    try {
      db.none(`DELETE FROM consolidated_residual_details;`)
        .then(() => {
          const cs = new pgp.helpers.ColumnSet(['indicator', 'yearmonth', 'platformid', 'merchantnumber', 'merchantname', 'level1', 'level2', 'level3', 'element', 'buyrate', 'merchantbillingcode', 'merchantbillingcodename', 'revn_byrt_in', 'clientormerchant_in', 'volume', 'amount', 'bnkid', 'date'], { table: 'consolidated_residual_details' });

          const query = pgp.helpers.insert(csvData, cs);

          db.none(query)
            .then(() => {
              db.one(`SELECT COUNT(*) FROM consolidated_residual_details WHERE date = '${csvData[0].date}';`)
                .then((result) => {
                  resolve(result.count);
                })
                .catch(error => {
                  reject(error);
                });
            })
            .catch(error => {
              reject(error);
            });
        })
        .catch(error => {
          reject(error);
        });
    }
    catch (err) {
      reject(err);
    }
  });
};

exports.selectResultsFull = async () => {
  return new Promise((resolve, reject) => {
    try {
      db.many(queries.selectResultsFull)
        .then((result) => {
          resolve(result);
        })
        .catch(error => {
          reject(error);
        });
    }
    catch (err) {
      reject(err);
    }
  });
}

exports.selectResultsSum = async () => {
  return new Promise((resolve, reject) => {
    try {
      db.task('get-sums', async t => {
        const amtExpenseSum = await t.one(queries.selectResultsSum('amount', 'EXPENSE'));
        const amtRevenueSum = await t.one(queries.selectResultsSum('amount', 'REVENUE'));
        const amtAfterCostExpenseSum = await t.one(queries.selectResultsSum('amount_after_cost', 'EXPENSE'));
        const amtAfterCostRevenueSum = await t.one(queries.selectResultsSum('amount_after_cost', 'REVENUE'));

        return { amtExpenseSum, amtRevenueSum, amtAfterCostExpenseSum, amtAfterCostRevenueSum };
      })
        .then(({ amtExpenseSum, amtRevenueSum, amtAfterCostExpenseSum, amtAfterCostRevenueSum }) => {
          resolve({ amtExpenseSum, amtRevenueSum, amtAfterCostExpenseSum, amtAfterCostRevenueSum });
        })
        .catch(error => {
          reject(error);

        });
    }
    catch (err) {
      reject(err);
    }
  });
}


exports.runNatesQuery = async () => {
  return new Promise((resolve, reject) => {
    try {
      db.none(queries.natesInsertAndUpdates)
        .then(() => {
          resolve();
        })
        .catch(error => {
          reject(error);
        });
    }
    catch (err) {
      reject(err);
    }
  });
}

exports.reset = async () => {
  return new Promise((resolve, reject) => {
    try {
      db.none(`DELETE FROM consolidated_residual_details;`)
        .then(() => {
          resolve();
        })
        .catch(error => {
          reject(error);
        });
    }
    catch (err) {
      reject(err);
    }
  });
};