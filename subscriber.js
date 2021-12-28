const Broker = require('./broker');
const MONGO_URL='mongodb://localhost:27017';

let options = {
    url: MONGO_URL,
    dbname: 'broker',
    name: 'StockMarket'
}

Broker.create(options).then(async (broker) => {
    await broker.subscribe("BSE", (data) => {
        let datetime = new Date();
        console.log(datetime, " data received from ", options.dbname, " for ", options.name, "----->", data.message)
    });
    //broker.destroy();
}).catch(e => {
    console.log('broker creation failed', e)
});