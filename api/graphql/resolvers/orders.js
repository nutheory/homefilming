const { createResolver } = require('apollo-resolvers')
const { baseResolver, isAuthenticated, isAuthorized, isAgent } = require('./auth')
const { Validate } = require('../../utils/validation')
const { create, update, joinOrLeave, destroy, order, orders, missions, uploaded } = require('../../services/orders')
const { createAgent } = require('./users')
const chalk = require('chalk')

const getOrders = isAuthenticated.createResolver(
  async (root, { input }, { user }) => {
    console.log(chalk.blue.bold("ORDERS?"), input)
    const ordersCollection = await orders({ usr: user, attrs: input })
    return ordersCollection
  }
)

const getOrder = isAuthenticated.createResolver(
  async (root, { input }, { user }) => {
    const orderById = await order({ usr: user, id: input.id })
    return orderById
  }
)

const getMissions = isAuthenticated.createResolver(
  async (root, { input }, { user }) => {
    console.log(chalk.blue.bold("MISSONS? input"),input)
    const queryParams = {}
    queryParams.sortKey = input.sortKey || "distanceFromLocation"
    queryParams.sortValue = input.sortValue || "ASC"
    queryParams.sizeLimit = input.sizeLimit || 20
    queryParams.colOffset = input.colOffset || 0
    const openMissions = await missions({ usr: user, qryPrms: queryParams })
    return openMissions
  }
)

const createOrderWithUser = baseResolver.createResolver(
  async (root, args, req) => {
    const newUser = await createAgent(root, args, req)
    req.user = newUser.user
    const result = await createOrder(root, args, req)
    // sendEmailConfirmationToUser,
    return { order: result.order, auth: newUser.auth }
  }
)

const createOrder = isAgent.createResolver(
  async (root, { input }, { user }) => {
    const order = await create({ usr: user, pln: input.order.plan, addr: input.order.address })
    // sendEmailConfirmationToUser,
    order.agent = user
    return { order }
  }
)

const updateOrder = isAuthorized.createResolver(
  async (root, { input }, { user }) => {
    const valid = await Validate( input ).isValidAddress().done()
    if( !valid ){ return valid }
    const order = await update({ usr: user, id: input.id, ordr: input.order, addr: input.address })
    return { order }
  }
)

const joinOrLeaveCollaboration = isAuthenticated.createResolver(
  async (root, { input }, { user }) => {
    const result = await joinOrLeave({ usr: user, id: input.id, updates: input })
    // sendEmailConfirmationToUser,
    console.log(chalk.blue.bold("joinOrLeave"), result)
    return result
  }
)

const uploadedOrder = isAuthorized.createResolver(
  async (root, { input }, { user }) => {
    console.log(chalk.blue.bold("RESOLVER"))
    const result = await uploaded({ usr: user, id: input.id, updates: input })

    return result
  }
)

const destroyOrder = isAuthorized.createResolver(
  async (root, { input }, { user }) => {
    const result = await destroy( input.id )
    return result
  }
)

const orderResolvers = {

  Query: {
    getOrder,
    getOrders,
    getMissions
  },

  Mutation: {
    createOrderWithUser,
    createOrder,
    joinOrLeaveCollaboration,
    uploadedOrder,
    updateOrder,
    destroyOrder
  }

}

module.exports = { orderResolvers }
