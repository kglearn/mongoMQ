const SIZE=1000000;
const MAX_QSIZE=1000;
const {MongoClient}=require('mongodb');

class Broker{
    constructor(client,option) {
        this.option=option;
        this.client=client;
    }

    static async create(option) {
        let client = null;
        try {
            client = await MongoClient.connect(option.url,{useUnifiedTopology: true });
            const db = client.db(option.dbname);
            option.qsize=option.qsize||MAX_QSIZE;

            let exist=await db.listCollections({ name: option.name }).hasNext();
            if(!exist) {
                let result=await db.createCollection(option.name, {capped: true, size: SIZE,max:option.qsize});
            }

            let broker=new Broker(client,option);
            return broker;
        } catch(e) {
            console.log('broker creation failed ',e)
            if (!!client) {
                client.close();
            }
            throw e;
        }
    }

    async subscribe(routingkey, next) {
        var filter = {routingkey:routingkey};

        if('function' !== typeof next) throw('Callback function not defined');

        let db=this.client.db(this.option.dbname);
        let collection=await db.collection(this.option.name);
        var cursorOptions = {
                                tailable: true,
                                awaitdata: true,
                                numberOfRetries: -1
                    };
        const tailableCursor = collection.find(filter, cursorOptions);
        var stream =tailableCursor.stream();
        console.log('queue is waiting for message ...');
        stream.on('data', next);
    }

    async publish(routingkey, message) {
        let data = {};
        data.routingkey = routingkey;
        data.message = message;
        data._id = new Date().getTime();
        let db = this.client.db(this.option.dbname);
        let result = await db.collection(this.option.name).insertOne(data);
        console.log(result);
        if(result.acknowledged==true) {
            console.log('message published to exchange ',this.option.name," with routing  key ",routingkey );
        }
        return result;
    }

    async destroy() {
        if(this.client) {
            this.client.close()
        }
    }
}

module.exports = Broker;