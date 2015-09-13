const rabbit = require('rabbit.js')
const _ = require('lodash')
import uuid from 'node-uuid'
import amqp from 'amqplib'
import Promise from 'bluebird'

import PersistenceHandler from './handlers/persistence'

/**
 * Implementation of the sails-rabbitmq Adapter
 */
const Adapter = {


  /**
   * Set the primary key datatype for the persistence datastore from config/rabbit.js.
   */
  pkFormat: _.get(global, ['sails', 'config', 'rabbitmq', 'pkFormat'], 'integer'),

  /**
   * Local connections store
   */
  connections: new Map(),

  /**
   * Adapter default configuration
   */
  defaults: {
    url: 'amqp://localhost',
    schema: false
  },

  persistenceCallbacks: new Set(),

  /**
   * persistence callback  queue name
   */
  persistenceCallbackQueue: 'sails.models.callback',

  /**
   * This method runs when a model is initially registered
   * at server-start-time.  This is the only required method.
   *
   * @param  {[type]}   connection [description]
   * @param  {[type]}   collection [description]
   * @param  {Function} cb         [description]
   * @return {[type]}              [description]
   */
  registerConnection(connection, collections, cb) {
    if (!connection.identity) return cb(new Error('Connection is missing an identity.'))
    if (this.connections.get(connection.identity)) return cb(new Error('Connection is already registered.'))

    //let context = rabbit.createContext(connection.url)

    let ok = amqp.connect(connection.url)


    ok = ok.then(conn => {

      return conn.createChannel()
        .then(ch => {
          return [conn, ch];
        })
    })

    ok = ok.then(([conn, ch]) => {
      let config = {
        identity: connection.identity,
        rabbitConnection: conn,
        channel: ch,
        models: new Map(_.pairs(collections)),
        persistence: connection.persistence
      }

      this.connections.set(connection.identity, config)
      return config
    })

    ok.then(config => {
      // each model needs to create its worker queue
      this.attachModelQueues(connection, collections, config)
        .then(() => {
          cb()
        })
        .catch(cb)
    })
  },

  attachModelQueues(connection, collections, config) {
    return this.setupPersistenceCallbackQueue(connection.identity)
      .then(() => {
        return Promise
          .all(_.map(collections, model => {
            return Promise.all([
              this.setupWorkerQueue(connection.identity, model.identity, config)
            ])
          }))
          .then(() => {
            if (config.persistence) {
              return Promise.all(_.map(collections, model => {
                new PersistenceHandler(connection, model)
              }))
            }
          })
          .then(() => {
            return Promise.resolve()
          })
      })
  },

  /**
   * create a queue for each model
   * any create/update events will be pushed into this queue
   * this is the same queue that the persistence handler will be listening on
   */
  setupWorkerQueue(connectionName, modelName, config) {
    return config.channel.assertQueue(this.getQueueName(modelName, 'persistence'))
  },

  /**
   * note that connectionName and modelName are bound when calling via this.model
   */
  persistenceResponse(connectionName, modelName, replyTo, modelInstance, correlationId) {
    let config = this.connections.get(connectionName)

    return config.channel.sendToQueue(replyTo,
      new Buffer(JSON.stringify(modelInstance)), {
        correlationId: correlationId
      })
  },

  consumePersistenceQueue(connectionName, modelName, handler) {
    let config = this.connections.get(connectionName)
    let queueName = this.getQueueName(modelName, 'persistence')
    return config.channel.consume(queueName, handler, {
      noAck: true
    })
  },

  /**
   * Fired when a model is unregistered, typically when the server
   * is killed. Useful for tearing-down remaining open connections,
   * etc.
   *
   * @param  {Function} cb [description]
   * @return {[type]}      [description]
   */
  teardown(conn, cb) {
    if (_.isFunction(conn)) {
      cb = conn
      conn = null
    }

    let connections = conn ? [conn].values() : this.connections.values()

    for (let c of connections) {
      c.rabbitConnection.close()
      this.connections.delete(c.identity)
    }
    cb()
  },

  /**
   * @override
   */
  create(connection, collection, values, cb) {
    this.update(connection, collection, {
      where: values
    }, values, cb)
  },

  /**
   * @override
   */
  update(connection, collection, criteria, values, cb) {
    let config = this.connections.get(connection)
    let corrid = uuid.v4()
    let persistenceQueueName = this.getQueueName(collection, 'persistence')


    this.persistenceCallbacks[corrid] = cb

    let reply = (msg) => {
      let cb = this.persistenceCallbacks[msg.properties.correlationId]
      console.log('the cb!', cb)
      cb(null, JSON.parse(msg.content.toString()))
    }

    config.rabbitConnection.createChannel().then(channel => {
        channel.consume(this.persistenceCallbackQueue, reply, {
          noAck: true
        })
      })
      .then(console.log.bind(console, 'result of consume?'))

    return config.channel.sendToQueue(persistenceQueueName, new Buffer(JSON.stringify(values)), {
      correlationId: corrid,
      replyTo: this.persistenceCallbackQueue
    })

  },

  /**
   * Publish a message to an exchange. Accepts the same arguments as the
   * update() method in the "semantic" interface.
   *
   * Exchange configuration defined in the adapter's connection config object
   * determines how the message is handled/routed by the exchange.
   */
  publish(connection, collection, values, cb) {
    console.log('calling publish')
  },

  /**
   * Setup and connect a PUBLISH socket for the specified model
   */
  getPublishSocket(connection, collection) {
    let config = this.connections.get(connection)
    let context = config.context
    let address = this.getExchangeName(collection)
    let socket = context.socket('PUBLISH', {
      routing: 'topic'
    })

    return new Promise((resolve, reject) => {
      socket.connect(address, () => {
        resolve(socket)
      })
    })
  },

  /**
   * Setup and connect a SUBSCRIBE socket for the specified model
   */
  getSubscribeSocket(connection, collection, options = {}) {
    let config = this.connections.get(connection)
    let context = config.context
    let address = this.getExchangeName(collection)
    let routingKey = this.getRoutingKey(connection, collection, options.where)
    let socket = context.socket('SUBSCRIBE', {
      routing: 'topic'
    })

    socket.setEncoding('utf8')
    socket.once('close', () => {
      config.userSockets.delete(socket)
    })

    return new Promise((resolve, reject) => {
      socket.connect(address, routingKey, () => {
        config.userSockets.add(socket)
        resolve(socket)
      })
    })
  },


  setupPersistenceCallbackQueue(connection) {
    let config = this.connections.get(connection)
      //TODO do we need a different callback queue for each model?
    return config.channel.assertQueue(this.persistenceCallbackQueue)
  },

  /**
   * creates or gets reference to two queues
   *   1. model.persistence.worker.queue
   *   2. model.persistence.callback.queue
   */
  getPersistenceQueue(connection, modelname, options = {}) {
    let config = this.connections.get(connection)

    if (config.queues.persistence.has(modelname)) {
      return Promise.resolve(config.queues.persistence.get(modelname))
    }

    return config.channel.assertQueue(this.getQueueName(modelname, options.name), {
        durable: true
      })
      .then(({
        queue
      }) => {
        config.queues.persistence.set(modelname, queue)
        return queue
      })
  },

  /**
   * Setup and connect a PUSH socket for the specified model
   */
  getPushSocket(connection, collection, options = {}) {
    let config = this.connections.get(connection)
    let context = config.context
    let address = this.getQueueName(collection, options.name)
    let socket = context.socket('PUSH')

    return new Promise((resolve, reject) => {
      socket.connect(address, () => {
        resolve(socket)
      })
    })
  },

  /**
   * Setup and connect a WORKER socket for the specified model
   */
  getWorkerSocket(connection, collection, options = {}) {
    let config = this.connections.get(connection)
    let context = config.context
    let address = this.getQueueName(collection, options.name)
    let socket = context.socket('WORKER')

    socket.setEncoding('utf8')
    socket.once('close', () => {
      config.userSockets.delete(socket)
    })
    return new Promise((resolve, reject) => {
      socket.connect(address, () => {
        config.userSockets.add(socket)
        resolve(socket)
      })
    })
  },

  /**
   * Return an extant socket of the specific type for the specified model
   */
  getSocket(connectionId, collection, type) {
    let connection = this.connections.get(connectionId)
    return connection.sockets[type].get(collection)
  },

  /**
   * Return the name of the AMQP exchange that is used by the specified model
   */
  getExchangeName(model) {
    return `sails.models.${model}`
  },

  /**
   * Return the name of the AMQP queue that is used by the specified model
   * in conjuction with a particular type of work(er)
   */
  getQueueName(model, name) {
    if (_.isUndefined(name)) {
      throw new Error('name cannot be undefined in getQueueName')
    }
    return `sails.models.${model}.${name}`
  },

  /**
   * Return AMQP routing key for a given Model instance
   * @return dot-delimited string of model attributes which constitutes the
   *    queue routing key
   */
  getRoutingKey(connection, collection, values) {
    let config = this.connections.get(connection)
    let Model = config.models.get(collection)
    if (_.isUndefined(values)) {
      return '#'
    } else if (!_.isArray(Model.routingKey)) {
      throw new Error(
        `The model ${Model.identity} must define a routingKey
        in order to be used with the Waterline pubsub interface`
      )
    } else {
      return this.parseRoutingKey(Model.routingKey, values)
    }
  },

  /**
   * @return a rabbitmq routing key derived from a list of model attributes
   */
  parseRoutingKey(routingKey, values) {
    return routingKey.map(attribute => {
      return values[attribute]
    }).join('.')
  },

  /**
   * Return a model's connection that will be used for persistence, if it
   * exists.
   */
  getPersistenceConnection(connection, collection) {
    let config = this.connections.get(connection)
    let Model = config.models.get(collection)

    let persistenceConnections = _.without(Model.connection, connection)

    return persistenceConnections[0]
  }
}

_.bindAll(Adapter)

export
default Adapter
