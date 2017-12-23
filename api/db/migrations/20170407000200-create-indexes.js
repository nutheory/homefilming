'use strict'

module.exports = {
  up: function (queryInterface, Sequelize) {
    queryInterface.addIndex('Users', ['email', 'companyId'])
    queryInterface.addIndex('Orders', ['pilotId', 'receiptId', 'agentId', 'editorId'])
    queryInterface.addIndex('Addresses', ['addressableId', 'addressable'])
    queryInterface.addIndex('Companies', ['name'])
    queryInterface.addIndex('Contacts', ['contactableId', 'contactable'])
    queryInterface.addIndex('Invitations', ['userId', 'mailId'])
    queryInterface.addIndex('Assets', ['assetableId', 'assetable', 'awsId'])
    queryInterface.addIndex('Ratings', ['ratableId', 'ratable'])
    queryInterface.addIndex('Notes', ['notableId', 'notable', 'authorId'])
    queryInterface.addIndex('Notifications', ['userId'])
  },

  down: function (queryInterface, Sequelize) {
    queryInterface.removeIndex('Users', ['email', 'companyId'])
    queryInterface.removeIndex('Orders', ['pilotId', 'receiptId', 'agentId', 'editorId'])
    queryInterface.removeIndex('Addresses', ['addressableId', 'addressable'])
    queryInterface.removeIndex('Companies', ['name'])
    queryInterface.removeIndex('Contacts', ['contactableId', 'contactable'])
    queryInterface.removeIndex('Invitations', ['userId', 'mailId'])
    queryInterface.removeIndex('Assets', ['assetableId', 'assetable', 'awsId'])
    queryInterface.removeIndex('Ratings', ['ratableId', 'ratable'])
    queryInterface.removeIndex('Notes', ['notableId', 'notable', 'authorId'])
    queryInterface.removeIndex('Notifications', ['userId'])
  }
}