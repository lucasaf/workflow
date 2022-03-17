const { sendMessage } = require("../clients/rabbitMQClient");
const { logger } = require("./logger");

const processStateListener = async (processState) => {
  logger.debug("called processStateListener");
  logger.info(`>>> POSTED PS ON PROCESS TOPIC: ${processState.id}`);
};

const activityManagerListener = async (activityManager) => {
  logger.debug("called activityManagerListener");

  /*     //console.log('listener: ', activityManager);
  
    const event = [
      {
        status: activityManager._status,
        action: activityManager._props.action,
        processId: activityManager._process_id,
        activityManagerId: activityManager._id,
        eventType: "activity_manager",
      },
    ];
  
    await sendEvent(event);
  
    const message = {
      processId: activityManager._process_id,
      activityManagerId: activityManager._id,
      status: activityManager._status,
    };
  
    await sendMessage(topic, message); 
  */

  const topic = 'flowbuild';
  const message = {
    input: {
      activityManagerId: activityManager._id,
      processId: activityManager._process_id,
      ...activityManager._props.result,
    },
    action: activityManager._props.action,
    schema: activityManager._parameters
  };

  sendMessage(topic, JSON.stringify(message))

  logger.info(`>>> POSTED AM ON PROCESS TOPIC: ${activityManager._process_id}`);
};

const activateNotifiers = (engine) => {
  engine.setProcessStateNotifier(processStateListener);
  engine.setActivityManagerNotifier(activityManagerListener);
};

module.exports = {
  activateNotifiers,
};
