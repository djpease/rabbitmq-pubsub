import * as amqp from "amqplib";
import { IRabbitMqConnectionFactory } from "./connectionFactory";
import * as Logger from "bunyan";
import * as Promisefy from "bluebird";
import { IQueueNameConfig, asPubSubQueueNameConfig } from "./common";
import { createChildLogger } from "./childLogger";

export interface IRabbitMqSubscriberDisposer {
    (): Promisefy<void>;
}

let subscriberChannelMap = {};

export class RabbitMqSubscriber {

    constructor(private logger: Logger, private connectionFactory: IRabbitMqConnectionFactory) {
        this.logger = createChildLogger(logger, "RabbitMqConsumer");
    }

    subscribe<T>(queue: string | IQueueNameConfig, action: (message: T) => Promisefy<any> | void): Promisefy<IRabbitMqSubscriberDisposer> {
        const queueConfig = asPubSubQueueNameConfig(queue);
        let _this = this;
        return this.connectionFactory.create()
            .then(function (connection) {
                // fix issue with new channels being created every subscribe
                let queueName = queueConfig.name,
                    currentChannel = subscriberChannelMap[queueName];
                //console.log('')
                if(currentChannel){
                    //console.log('Using EXISTING subscriber channel!')
                    return currentChannel;
                }else {
                    //console.log('create a new subscriber channel')
                    return connection.createChannel();
                }

            })
            .then(function (channel) {
                let queueName = queueConfig.name;
                if(!subscriberChannelMap[queueName]){
                    subscriberChannelMap[queueName] = channel;
                }
                _this.logger.trace("got channel for queue '%s'", queueConfig.name);
                return _this.setupChannel<T>(channel, queueConfig)
                    .then((queueName) => {
                        _this.logger.debug("queue name generated for subscription queue '(%s)' is '(%s)'", queueConfig.name, queueName);
                        let queConfig = { ...queueConfig, dlq: queueName};
                        return _this.subscribeToChannel<T>(channel, queueConfig, action)})
            });
    }

    private setupChannel<T>(channel: amqp.Channel, queueConfig: IQueueNameConfig) {
        this.logger.trace("setup '%j'", queueConfig);
        return this.getChannelSetup(channel, queueConfig);
    }

    private subscribeToChannel<T>(channel: amqp.Channel, queueConfig: IQueueNameConfig, action: (message: T) => Promisefy<any> | void) {
        this.logger.trace("subscribing to queue '%s'", queueConfig.name);
        return channel.consume(queueConfig.dlq, (message) => {
            let msg: T;
            Promisefy.try(() => {
                msg = this.getMessageObject<T>(message);
                this.logger.trace("message arrived from queue '%s' (%j)", queueConfig.name, msg)
                return action(msg);
            }).then(() => {
                this.logger.trace("message processed from queue '%s' (%j)", queueConfig.name, msg)
                channel.ack(message)
            }).catch((err) => {
                this.logger.error(err, "message processing failed from queue '%j' (%j)", queueConfig, msg);
                channel.nack(message, false, false);
            });
        }).then(opts => {
            this.logger.trace("subscribed to queue '%s' (%s)", queueConfig.name, opts.consumerTag)
            return (() => {
                this.logger.trace("disposing subscriber to queue '%s' (%s)", queueConfig.name, opts.consumerTag)
                return Promisefy.resolve(channel.cancel(opts.consumerTag)).return();
            }) as IRabbitMqSubscriberDisposer
        });
    }

    protected getMessageObject<T>(message: amqp.Message) {
        return JSON.parse(message.content.toString('utf8')) as T;
    }

    protected async getChannelSetup(channel: amqp.Channel, queueConfig: IQueueNameConfig) {
        await channel.assertExchange(queueConfig.dlx, 'fanout', this.getDLSettings());
        let result =  await channel.assertQueue(queueConfig.dlq, this.getQueueSettings(queueConfig.dlx));
        await channel.bindQueue(result.queue, queueConfig.dlx, '');
        return result.queue;
    }

    protected getQueueSettings(deadletterExchangeName: string): amqp.Options.AssertQueue {
        return {
            exclusive: true
        }
    }

    protected getDLSettings(): amqp.Options.AssertQueue {
        return {
            durable: false
        }
    }
}
