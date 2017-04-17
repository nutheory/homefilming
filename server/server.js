var express = require('express')
const graphQLHTTP = require('express-graphql')
const bodyParser = require('body-parser')
const jwt = require('jsonwebtoken')
const path = require('path')
const passport = require('passport')
const morgan = require('morgan')
const models = require('./models')
const schema = require('./schema')
const config = require('./config')
const passportConfig = require('./services/auth')
const webpackMiddleware = require('webpack-dev-middleware')
const webpack = require('webpack')
const webpackConfig = require('../webpack.config.js')
var app = express()
const router = express.Router()

app.set('port', (process.env.PORT || 5000))
app.set('is_dev', (app.settings.env === 'development'))
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }))
app.use(morgan('dev'))
app.use(passport.initialize())
app.use(express.static(__dirname + '/dist'))
app.use('/graphql', graphQLHTTP({ schema, graphiql: true }))
app.use(webpackMiddleware(webpack(webpackConfig)))

app.get('*', (req, res) => {
  res.sendFile(path.resolve() + '/dist/index.html')
})

module.exports = app
