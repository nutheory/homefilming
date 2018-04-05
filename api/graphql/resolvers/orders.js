const { createResolver } = require('apollo-resolvers')
const { baseResolver, isAuthenticated, isAuthorized, isAgent } = require('./auth')
const { Validate } = require('../../utils/validation')
const { create, update, joinOrLeave, destroy, order, orders, approve, reject,
        missions, uploaded, notifyLocalPilots, gallery } = require('../../services/orders')
const { createAgent } = require('./users')
const { welcomeConfirmationMailer, confirmationMailer } = require('../../mailers/order')
const chalk = require('chalk')

const getGallery = baseResolver.createResolver(
  async (root, { input }, req) => {
    const result = await gallery({ uuid: input.uuid })
    return result
  }
)

const getOrders = isAuthenticated.createResolver(
  async (root, { input }, { user }) => {
    const ordersCollection = await orders({ usr: user, attrs: input })
    return ordersCollection
  }
)

const getOrder = isAuthenticated.createResolver(
  async (root, { input }, { user }) => {
    const result= await order({ usr: user, id: input.id })
    return result
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
    const result = await createOrder(root, args, req, true )
    welcomeConfirmationMailer({ order: result.order, user: newUser.user })
    return { order: result.order, auth: newUser.auth }
  }
)

const createOrder = isAgent.createResolver(
  async (root, { input }, { user }, newUser = false) => {
    const order = await create({ ordr: input.order, usr: user, addr: input.order.address })
    if( !newUser ){ confirmationMailer({ order, user }) }
    notifyLocalPilots({ ordr: order })
    order.agent = user
    return { order }
  }
)

const updateOrder = isAuthorized.createResolver(
  async (root, { input }, { user }) => {
    // const valid = await Validate( input ).isValidAddress().done()
    // if( !valid ){ return valid }
    const result = await update({ usr: user, id: input.id, ordr: input.order, addr: input.address })
    return result
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

const approveOrder = isAuthenticated.createResolver(
  async (root, { input }, { user }) => {
    const result = await approve({ user, id: input.id, photos: input.order.photos })
    return result
  }
)

const rejectOrder = isAuthenticated.createResolver(
  async (root, { input }, { user }) => {
    const result = await reject({ user, id: input.id })
    return result
  }
)

const uploadedOrder = isAuthorized.createResolver(
  async (root, { input }, { user }) => {
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
    getGallery,
    getOrder,
    getOrders,
    getMissions
  },

  Mutation: {
    createOrderWithUser,
    createOrder,
    approveOrder,
    rejectOrder,
    joinOrLeaveCollaboration,
    uploadedOrder,
    updateOrder,
    destroyOrder
  }

}

module.exports = { orderResolvers }
