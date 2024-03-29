'use strict'

const { createStripeCharge } = require('../../services/payments')
const Faker = require('faker')
const _ = require('lodash')
const db = require('../../models')
const nums = [ 4, 5, 6, 7 ]
const Addresses = require('../utils/Addresses.json')
const plans =  require('../../utils/pricing_plans.json')
const chalk = require('chalk')


module.exports = {
  up: async function (queryInterface, Sequelize) {

    const agents = await db.User.findAll({where: {type: 'agent'}}).catch(err => { console.log(chalk.blue.bold("order1"), err); throw err })
    const pilot = await db.User.findOne({where: {email: 'drush81+pilot@gmail.com'}}).catch(err => { console.log(chalk.blue.bold("order2"), err); throw err })
    const admin = await db.User.findOne({where: {email: 'drush81+admin@gmail.com'}}).catch(err => { console.log(chalk.blue.bold("order3"), err); throw err })
    const isNew = () => ({ status: 'recruiting' })
    const isFilming = () => {
      const distance = parseFloat(Math.min(10 + (Math.random() * (50 - 10)),50).toFixed(2))
      const bounty = Math.round(40 + distance)
      return { status: 'filming', pilotId: pilot.id, pilotAcceptedAt: new Date(),
      pilotBounty: bounty, pilotDistance: distance }
    }
    const isUploaded = () => {
      const distance = parseFloat(Math.min(10 + (Math.random() * (50 - 10)),50).toFixed(2))
      const bounty = Math.round(40 + distance)
      return { status: 'uploaded', pilotId: pilot.id, pilotAcceptedAt: new Date(), uploadedAt: new Date(),
      pilotBounty: bounty, pilotDistance: distance }
    }
    const isDelivered = () => {
      const distance = parseFloat(Math.min(10 + (Math.random() * (50 - 10)),50).toFixed(2))
      const bounty = Math.round(40 + distance)
      return { status: 'approved_completed', pilotId: pilot.id, pilotAcceptedAt: new Date(), uploadedAt: new Date(),
      pilotBounty: bounty, pilotDistance: distance, reviewedBy: admin.id, reviewedAt: new Date() }
    }

    const statuses = [ isNew, isNew, isNew, isNew, isNew, isNew, isNew, isFilming, isFilming, isFilming, isDelivered, isDelivered ]

    let orderlist = []

    await Promise.all(agents.map( async (agent) => {
      let agentId = agent.dataValues.id
      let customer = agent.dataValues.customerId
      let num_of_orders = _.sample(nums)
      let times = [...Array(num_of_orders).keys()]
      await Promise.all(times.map( async (i) => {
        let base = {
          customer,
          agentId: agentId,
          amountPaid: "34900",
          createdAt: new Date(),
          updatedAt: new Date()
        }
        let status = _.sample(statuses)
        let statusObj = status()
        console.log('statusObj', statusObj)
        let buildOrder = _.merge(base, statusObj)
        orderlist.push(buildOrder)
      }))
    }))

    await Promise.all(orderlist.map(async (order) => {
      let pln = _.sample(plans)
      let customerId = order.customer
      let amountPaid = "34900"
      order.plan = pln.name
      const newOrder = await db.sequelize.transaction(t => {
        return db.Order.create(order, { transaction: t, customer: customerId, pln, amountPaid })
          .then(ord => {
            const address = _.merge(_.sample(Addresses), { addressableId: ord.id, addressable: 'order' })
            return db.Address.create(address, {transaction: t}).then(address => {
              const orderT = _.merge(order, { address })
              return orderT
            }).catch(err => { console.log(chalk.blue.bold("order1"), err); throw err })
          }).catch(err => { console.log(chalk.blue.bold("order2"), err); throw err })
        }).catch(err => { console.log(chalk.blue.bold("order3"), err); throw err })
      return newOrder
    })).catch(err => { console.log(chalk.blue.bold("order4"), err); throw err })
  },

  down: function (queryInterface, Sequelize) {
    return queryInterface.bulkDelete('Orders', null, {})
  }
}
