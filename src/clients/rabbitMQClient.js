const { connect: connectRabbitMQ  } = require('amqplib');
const { logger } = require("./utils/logger");

let channel;
async function connect() {
  try {
    logger.info('[QUEUE] Trying to connect no RABBITMQ Broker');

    const password = 'password'
    const user = 'user'
    const host = 'host'

    const conn = await connectRabbitMQ(`amqp://${user}:${password}@${host}`);
    channel = await conn.createChannel();

    listenerSync(channel);

    logger.info('[QUEUE] connected to RABBITMQ Broker');
  } catch (error) {
    logger.error(`[QUEUE] Error at RABBITMQ connect ${error}`);
    logger.error(error);
  }
}

function sendMessage(queue, message) {
  if (channel) {
    channel.publish(queue, 'flow', Buffer.from(message))
  }
  logger.info('[QUEUE] Called sendMessage');
}

function listenerSync(channel){
  if(channel){
    // sendMessage('flowbuild', 'message from WF teste')

    channel.consume('flowbuild', (message) => {
      console.log(message.content.toString())
      channel.ack(message);
    });
  }
}

module.exports = {
  sendMessage: sendMessage,
  connectBroker: connect,
}