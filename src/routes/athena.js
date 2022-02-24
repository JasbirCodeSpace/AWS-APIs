const express = require("express");
const router = express.Router();
const AWS = require("aws-sdk");
const AthenaExpress = require("athena-express");
const ATHENA_S3_LOCATION = process.env.ATHENA_S3_LOCATION;

const awsCredentials = {
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY,
  secretAccessKey: process.env.AWS_SECRET_KEY,
};

AWS.config.update(awsCredentials);

const athenaExpressConfig = {
  aws: AWS,
  s3: ATHENA_S3_LOCATION,
  formatJson: true,
  getStats: true,
};
const athenaExpress = new AthenaExpress(athenaExpressConfig);

router.post("/database/create", async function (req, res) {
  try {
    const { databaseName } = req.body;
    let myQuery = {
      sql: `CREATE DATABASE IF NOT EXISTS ${databaseName};`,
    };

    athenaExpress
      .query(myQuery)
      .then((results) => {
        return res.json({
          status: "success",
          result: results,
        });
      })
      .catch((error) => {
        console.log(error);
        return res.json({
          status: "error",
          error: error,
        });
      });
  } catch (err) {
    console.log(err);
    return res.json({
      status: "error",
      error: err,
    });
  }
});

router.delete("/database/delete", async function (req, res) {
  try {
    const { databaseName } = req.body;
    let myQuery = {
      sql: `DROP SCHEMA IF EXISTS ${databaseName} CASCADE;`,
    };

    athenaExpress
      .query(myQuery)
      .then((results) => {
        return res.json({
          status: "success",
          result: results,
        });
      })
      .catch((error) => {
        console.log(error);
        return res.json({
          status: "error",
          error: error,
        });
      });
  } catch (err) {
    console.log(err);
    return res.json({
      status: "error",
      error: err,
    });
  }
});

router.post("/table/create", async function (req, res) {
  try {
    const { databaseName, tableName, columns, s3Location } = req.body;
    if (!s3Location.startsWith("s3://") || !s3Location.endsWith("/")) {
      throw new Error('s3Location must start with "s3://" and end with "/"');
    }
    if (s3Location.includes("*") || s3Location.includes("_")) {
      throw new Error('s3Location must not include "*" or "_"');
    }
    if (s3Location.includes("arn:aws:s3:")) {
      throw new Error("s3Location must not include ARN");
    }

    let myQuery = {
      sql: `CREATE EXTERNAL TABLE IF NOT EXISTS \`${tableName}\` (
                	${columns
                    .map((column) => `\`${column.name}\` ${column.type}`)
                    .join(" ,")}
                    )
					ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
					WITH SERDEPROPERTIES (
					'serialization.format' = ',',
					'field.delim' = ','
					)
					LOCATION '${s3Location}'
                    TBLPROPERTIES ("skip.header.line.count"="1")
                    ;
                    `,
      db: databaseName,
    };

    athenaExpress
      .query(myQuery)
      .then((results) => {
        return res.json({
          status: "success",
          result: results,
        });
      })
      .catch((error) => {
        console.log(error);
        return res.json({
          staus: "error",
          error: error,
        });
      });
  } catch (err) {
    console.log(err);
    return res.json({
      status: "error",
      error: err,
    });
  }
});

router.delete("/table/delete", async function (req, res) {
  const { databaseName, tableName } = req.body;
  try {
    let myQuery = {
      sql: `DROP TABLE IF EXISTS ${tableName};`,
      db: databaseName,
    };

    athenaExpress
      .query(myQuery)
      .then((results) => {
        return res.json({
          status: "success",
          result: results,
        });
      })
      .catch((error) => {
        console.log(error);
        return res.json({
          status: "error",
          error: error,
        });
      });
  } catch (err) {
    console.log(err);
    return res.json({
      status: "error",
      error: err,
    });
  }
});

router.post("/execute", async function (req, res) {
  try {
    const { databaseName, query } = req.body;
    let myQuery = {
      sql: query,
      db: databaseName,
    };

    athenaExpress
      .query(myQuery)
      .then((results) => {
        return res.json({
          status: "success",
          result: results,
        });
      })
      .catch((error) => {
        console.log(error);
        return res.json({
          status: "error",
          error: error,
        });
      });
  } catch (err) {
    console.log(err);
    return res.json({
      status: "error",
      error: err,
    });
  }
});

module.exports = router;
