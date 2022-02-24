require('dotenv').config({ path: './src/env/.env' });

const express = require('express');
const app = express();
const PORT = process.env.APP_PORT;

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('build'));

app.use('/athena', require('./src/routes/athena'));
app.listen(PORT, () => console.log('Server is running on port: ', PORT));