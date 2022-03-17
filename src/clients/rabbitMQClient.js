const { connect: connectRabbitMQ  } = require('amqplib');
const { logger } = require("../utils/logger");

const { BROKER_PASSWORD, BROKER_USER, BROKER_HOST } = process.env

let channel;
async function connect() {
  try {
    logger.info('[QUEUE] Trying to connect no RABBITMQ Broker');

    const conn = await connectRabbitMQ(`amqp://${BROKER_USER}:${BROKER_PASSWORD}@${BROKER_HOST}`);
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
      logger.info('[QUEUE] consume: ' + message.content.toString())
      channel.ack(message);
    });
  }
}

module.exports = {
  sendMessage: sendMessage,
  connectBroker: connect,
}