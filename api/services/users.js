const { task, of, waitAll } = require('folktale/concurrency/task')
const Result = require('folktale/result')
const compose = require('folktale/core/lambda/compose')
const { chain, map } = require('folktale/fantasy-land')
const Jwt = require('jsonwebtoken')
const Secret = process.env.JWT_SECRET
const R = require('ramda')
const db = require('../models')
const Op = db.Sequelize.Op
const { createError } = require('../utils/appErrors')
const { retrieveCustomer, addCustomerSource } = require('./payments')
const { FailFastError, NotFoundError } = require('../utils/errors')
const chalk = require('chalk')

const capitalize = str =>
  str.charAt(0).toUpperCase() + str.slice(1)

const collectionOptions = ({ sortKey, sortValue, sizeLimit, colOffset }) => {
  const obj = { distinct: true }
  if( sortKey && sortValue ){ obj.order = [[ sortKey, sortValue ]] }
  if( sizeLimit ){ obj.limit = sizeLimit }
  if( colOffset ){ obj.offset = colOffset }
  return obj
}

const userIncludes = (criteria) => {
  if( criteria && criteria.type === "pilot"){
    return { include: [{ model: db.Address, as: 'address' }, { model: db.Asset, as: 'avatars' },
      { model: db.Asset, as: 'insurances' }, { model: db.Asset, as: 'licenses' },
      { model: db.Contact, as: 'contacts' }, { model: db.FailedMission, as: 'failedMissions' }] }
  } else {
    return { include: [{ model: db.Address, as: 'address' }, { model: db.Asset, as: 'avatars' },
    { model: db.Contact, as: 'contacts' }] }
  }
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

const log = data => R.tap(() => console.log(chalk.blue.bold('DATA'), data), data)

const getFullUser = ({ attrs }) =>
  task(resolver =>
    db.User.findOne({ where: (attrs.id ? { id: attrs.id  } : { id: attrs.usr.id } ),
      include: [{ model: db.Address, as: 'address' }, { model: db.Asset, as: 'avatars' },
        { model: db.Asset, as: 'insurances' }, { model: db.Asset, as: 'licenses' },
        { model: db.Contact, as: 'contacts' }, { model: db.FailedMission, as: 'failedMissions' }] })
      .then(res => resolver.resolve({ usr: res, attrs }) )
      .catch(err => resolver.reject(FailFastError(err.name, { args: attrs, loc: 'Service: User.getFullUser' }))) )
  .run().promise()

const getCoreUser = ({ attrs }) => {
  return task(resolver =>
    db.User.find({ where: attrs.email ? { email: attrs.email.toLowerCase() } :
      (attrs.id ? { id: attrs.id  } : { id: attrs.usr.id } ),
      include: [{ model: db.Address, as: 'address' }, { model: db.Contact, as: 'contacts' },
      { model: db.Asset, as: 'avatar' }] })
      .then(usr => {
        console.log( chalk.blue.bold("ATTRS"), usr )
        return resolver.resolve({ usr, attrs }) 
      })
      .catch(err => console.log( chalk.blue.bold("BIG-ERR"), err ) ))
  .run().promise()
}

const getUser = ({ attrs }) =>
  task(resolver =>
    db.User.find({ where: { id: attrs.id } })
      .then(usr => resolver.resolve({ usr, attrs }))
      .catch(err => resolver.reject(FailFastError(err.name, { args: attrs, loc: 'Service: User.getUser' }))) )
  .run().promise()

const getUsersByCriteria = ({ attrs }) =>
  task(resolver =>
    db.User.findAndCountAll(R.merge({ where: R.merge(attrs.criteria,
      { [Op.or]: [
        { name: { [Op.iLike]: `%${attrs.queryString}%` } },
        { email: { [Op.iLike]: `%${attrs.queryString}%` } },
        { customerId: { [Op.iLike]: `%${attrs.queryString}%` } },
        { accountId: { [Op.iLike]: `%${attrs.queryString}%` } }
      ] }) }, R.merge(userIncludes(attrs.criteria), collectionOptions( attrs.options ))))
      .then(res => resolver.resolve({ count: res.count, users: res.rows }))
      .catch(err => resolver.reject(FailFastError(err.name, { args: attrs, loc: 'Service: User.getUser' }))) )
  .run().promise()

const contactAssociations = async (asscConts) => {
  console.log(chalk.blue.bold("err start"),asscConts.contacts)
  if(asscConts.contacts && asscConts.contacts.length > 0){
    const contacts = []
    asscConts.contacts.map(contact => {
      if(contact.status === "delete" || contact.content === ""){
        contacts.push( task(resolver =>
          db.Contact.destroy({ where: { id: contact.id }, transaction: asscConts.tx })
            .then(res => resolver.resolve(res.dataValues))))
      } else if(contact.status === "new"){
        contacts.push( task(resolver =>
          asscConts.usr.createContact({ type: contact.type, content: contact.content,
            default: contact.default }, { transaction: asscConts.tx })
          .then(res => resolver.resolve(res.dataValues))))
      } else {
        contacts.push( task(resolver =>
          db.Contact.update({ type: contact.type, content: contact.content, default: contact.default },
            { where: { id: contact.id }, transaction: asscConts.tx })
            .then(res => resolver.resolve(res.dataValues))))
      }})
    const result = await waitAll(contacts).run().promise()
    console.log(chalk.blue.bold("err end"),result)
    return { contacts: result }
  }
}

const createAssociations = (assc) => {
  const tasks = []
  R.keys(assc.attrs).map(attr =>
    tasks.push( task(resolver => {
      assc.usr['create' + capitalize(attr)](assc.attrs[attr], { transaction: assc.tx })
      .then(res => resolver.resolve({ [attr]: res.dataValues }))})))
  return waitAll(tasks).run().promise()
}

const createUserWithAssociations = attrs =>
  db.sequelize.transaction(tx =>
    task(resolver =>
      db.User.create(attrs, { stripeToken: attrs.stripeToken, transaction: tx })
      .then(async (usr) => {
        const addressPromise = attrs.address ? usr.createAddress(attrs.address, { transaction: tx }) : []
        const contactsPromises = attrs.contacts ? contactAssociations({usr, contacts: attrs.contacts, tx}) : []
        const [address, contacts] = await Promise.all([addressPromise, contactsPromises])
        resolver.resolve({ attrs: usr.dataValues })
      }).catch(err => resolver.reject(FailFastError(err.name, { args: { attrs }, loc: 'Service: User.createUserWithAssociations' }))) )
    .run().promise() ).catch(err => { throw err })

const updateAssociations = ({ usr, attrs }) => {
  return db.sequelize.transaction(tx =>
    task(async (resolver) => {
      console.log(chalk.blue.bold("attrs"),attrs.user)
      const addressPromise = attrs.user && attrs.user.address && usr.address ?
        usr.address.updateAttributes(attrs.user.address, { transaction: tx }) : []
      const contactsPromises = attrs.user && attrs.user.contacts ? contactAssociations({usr, contacts: attrs.user.contacts, tx}) : []
      const [address, contacts] = await Promise.all([addressPromise, contactsPromises])
      resolver.resolve({ usr, attrs })
    })
    .orElse( reason => reason ).run().promise() ).catch(err => { throw err })
}

const updateUser = ({ usr, attrs }) =>
  db.sequelize.transaction(tx =>
    task(resolver =>
      usr.update(attrs.user, { transaction: tx })
        .then(usr => resolver.resolve({ usr, attrs }))
        .catch(err => { throw err }))
    .orElse( reason => reason ).run().promise() ).catch(err => { console.log(chalk.blue.bold("err "),err) })

const validateIncomingPassword = async ({ usr, attrs }) => {
  console.log("USSR", usr)
  if(!usr){ throw new Error(NotFoundError({ args: attrs, loc: 'Service: User.validateIncomingPassword'})) }
  return usr.comparePassword(attrs.password).then(res => {
    console.log(chalk.blue.bold("still OK"),res)
    if(res){
      return { usr }
    } else {
      console.log(chalk.blue.bold("still OK2"),res)
      throw new Error(FailFastError("AuthenticationFailed", { args: attrs, loc: 'Service: User.validateIncomingPassword' }))
    }
  }).catch(err => {
    throw new Error(FailFastError("AuthenticationFailed", { args: attrs, loc: 'Service: User.validateIncomingPassword' })) })
}

const returnTokenAndUserInfo = userObj => db.User.createAndReturnToken(userObj)

const generatePasswordReset = async ({ usr }) => {
  const token = await Jwt.sign({ id: usr.id, email: usr.email }, Secret, { expiresIn: '1h' })
  const update = await usr.update({ passwordResetToken: token, passwordResetSent: Date.now() })
  return { user: usr.dataValues, token }
}

const passwordReset = async ({ usr, attrs }) =>
  db.sequelize.transaction(tx =>
    task(resolver => usr.update({
      password: attrs.password,
      passwordResetToken: null,
      passwordResetSent: null }, { transaction: tx })
      .then(usr => resolver.resolve({ usr }))
      .catch(err => resolver.reject({ err })))
    .orElse( reason => reason ).run().promise() ).catch(err => { console.log(chalk.blue.bold("err "),err) })

const destroyUser = ({ usr }) => {
  const cl = R.clone(usr)
  return task(resolver =>
    usr.destroy()
    .then(res => resolver.resolve({ usr: cl }))
    .catch(err => resolver.reject(FailFastError(err.name, { args: cl, loc: 'Service: User.destroyUser' }))) )
  .run().promise()
}

const deactivateUserToggle = ({ usr, attrs }) =>
  db.sequelize.transaction(tx =>
    task(resolver => usr.update({
      deactivated: !usr.deactivated,
      deactivatedReason: attrs.deactivatedReason ? attrs.deactivatedReason : null,
      refreshToken: true }, { transaction: tx })
      .then(res => resolver.resolve({ usr: res }))
      .catch(err => resolver.reject(FailFastError(err.name, { args: {}, loc: 'Service: User.deactivateUser' }))) )
    .run().promise() )

const returnUser = ({ usr }) => {
  if( !usr ){ throw NotFoundError({ args: usr, loc: 'Service: User.returnUser' }) }
  return { user: usr.dataValues }
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

const profile = R.pipeP(
  getFullUser,
  returnUser
)

const collection = getUsersByCriteria

const login = R.pipeP(
  getCoreUser,
  validateIncomingPassword,
  returnTokenAndUserInfo
)

const create = R.tryCatch(R.pipeP(
  createUserWithAssociations,
  getCoreUser,
  returnTokenAndUserInfo
), log)

const update = R.pipeP(
  getUser,
  updateUser,
  getFullUser,
  updateAssociations,
  getCoreUser,
  returnTokenAndUserInfo
)

const destroy = R.pipeP(
  getUser,
  destroyUser,
  returnUser
)

const verify = R.pipeP(
  getUser,
  updateUser,
  returnUser
)

const refresh = R.pipeP(
  getUser,
  returnUser
)

const initReset = R.pipeP(
  getCoreUser,
  generatePasswordReset
)

const reset = R.pipeP(
  getCoreUser,
  passwordReset,
  returnTokenAndUserInfo
)

const deactivate = R.pipeP(
  getUser,
  deactivateUserToggle,
  returnTokenAndUserInfo
)

module.exports = { create, update, verify, destroy, profile, login, refresh, collection, initReset, reset, deactivate }
