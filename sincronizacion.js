const feathers = require('@feathersjs/feathers')
const rest = require('@feathersjs/rest-client')
const axios = require('axios')
const configuracion = require('./configuracion')

const main = {
    getService: (serviceName) => {
        const app = feathers()
        const cliente = rest(configuracion.url)
    
        app.configure(cliente.axios(axios))
    
        return app.service(serviceName)
    }
}

module.exports = main