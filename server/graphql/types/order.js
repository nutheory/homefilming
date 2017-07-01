import Address from './address'
import User from './user'

const Order = `

  type Order {
    id: ID
    stripeInfo: String
    saveCard: Boolean
    plan: String
    status: String
    userId: String
    timeOfDay: String
  }

  input CreateOrderInput {
    stripeInfo: String
    saveCard: Boolean
    plan: String
    status: String
    address: CreateOrderAddressInput
    user: CreateOrderUserInput
  }

  type CreateOrderPayload {
    id: ID
    plan: String
    receiptId: String
    status: String
    address: CreateOrderAddressPayload
    user: CreateOrderUserPayload
  }
`

export default [Order, Address, User]