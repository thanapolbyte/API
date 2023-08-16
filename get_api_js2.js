// pm.environment.set("BRAND_CODE", "CEGID");
const CryptoJS = require('crypto-js');
const moment = require('moment');
const axios = require('axios');
const sql = require('mssql');

// brand = pm.environment.get("BRAND_CODE");
const body = '{"brandCode":"CEGID"}';
// pm.environment.set("BODY",body);

const clientCode = "MINORPLUS";
const keyCode = "MINORPLUS_CEGID";

const hashkey = "6ZtuZq9h66HgIEzSgCa73LPQe6wgPDYxnhcZ4WoXmR1rFrrvrOxvKUoSUN180JQ3IblywSQpbG6YBizx9TfvrVQcPFsa7L1DCQke6iIakJfE55/iw/RfkUsCwio54k9rXf5SNMHNZFin6RY4Tq1WvXHmgyT+7qB0YaBYI2qBGco=";

const token = getMessageSignature(clientCode, keyCode, body, hashkey);

function getMessageSignature(clientCode, keyCode, request, secret) {
  const uri = '/authen/v1/clientToken/generateNew';
  const timestamp = moment().format("yyyyMMDDHHmmss");
  const method = 'POST';
  const tokenType = 'HS';
  const clientLibVersion = "1.0.0";

  const headerInfo = {
    "clientCode": clientCode,
    "clientLibVersion": clientLibVersion,
    "keyCode": keyCode,
    "method": method,
    "timestamp": timestamp,
    "tokenType": tokenType,
    "uri": uri,
  };

  const rawstr = clientCode + clientLibVersion + keyCode + method + timestamp + tokenType + uri + request;

  const message = JSON.stringify(headerInfo);
  const secret_buffer = CryptoJS.enc.Base64.parse(secret);
  const hmac = CryptoJS.algo.HMAC.create(CryptoJS.algo.SHA512, secret_buffer);
  hmac.update(rawstr, secret_buffer);
  const hmacValue = hmac.finalize().toString(CryptoJS.enc.Hex);

  const header = CryptoJS.enc.Base64.stringify(CryptoJS.enc.Utf8.parse(message));
  const token = CryptoJS.enc.Base64.stringify(CryptoJS.enc.Utf8.parse(header + ":" + hmacValue));

  return CryptoJS.enc.Base64.stringify(CryptoJS.enc.Utf8.parse(header + ":" + hmacValue));
}

const options = {
  method: 'POST',
  url: 'https://sit-lifestyle-api.deepblok.io/v1/icrm/core/authen',
  headers: {
    Authorization: 'Basic ' + token,
    'Content-Type': 'application/json'
  },
  data: {
    brandCode: 'CEGID'
  }
};

axios(options)
  .then(response => {
    const authToken = response.data.data.authToken;
    const dateOfExpire = response.data.data.dateOfExpire;
    console.log('authToken:', authToken);
    console.log('dateOfExpire:', dateOfExpire);

    // Call the updateAuthToken function with the new authToken
    updateAuthToken(authToken);
  })
  .catch(error => {
    throw new Error(error);
  });

// Configure the SQL Server connection
const config = {
  user: 'trading',
  password: 'Tr@ding',
  server: 'SMDC2RCEGDB02',
  database: 'MinorPOS',
  options: {
    encrypt: true // If using Azure SQL Database
  }
};

// Update the authToken in the database
async function updateAuthToken(authToken) {
  try {
    // Connect to the SQL Server
    await sql.connect(config);

    // Define the SQL query
    const query = `UPDATE DeepBLOK_config SET values_type = @authToken where Types = 'authToken'`;

    // Prepare the SQL statement
    const request = new sql.Request();
    request.input('authToken', sql.NVarChar, authToken);

    // Execute the SQL query
    const result = await request.query(query);

    console.log('AuthToken updated successfully');
  } catch (error) {
    console.error('Error updating AuthToken:', error);
  } finally {
    // Close the SQL Server connection
    sql.close();
  }
}
