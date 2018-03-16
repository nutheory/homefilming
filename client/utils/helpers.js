import Moment from 'moment'

const humanize = (str) => str
  .replace(/^[\s_]+|[\s_]+$/g, '')
  .replace(/[_\s]+/g, ' ')
  .replace(/^[a-z]/, (m) => m.toUpperCase())


const dateTimeShort = (date) => Moment(Date.parse(date)).format('MMM Do YYYY, h:mma')
const dateShort = (date) => Moment(Date.parse(date)).format('MMMM Do YYYY')

module.exports =  { humanize, dateTimeShort, dateShort }
