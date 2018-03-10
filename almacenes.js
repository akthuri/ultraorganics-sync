const connection = require('tedious').Connection
const request = require('tedious').Request
const TYPES = require('tedious').TYPES
const async = require('async')
const moment = require('moment')
const configuracion = require('./configuracion')
const sincronizacion = require('./sincronizacion')

const state = {
    conexion: null,
    almacenService: null
}

function main (err) {
    if (err) {
        console.log(err)
    } else {
        console.log('conectado')

        async.waterfall([leerAlmacenes, actualizarAlmacenes], complete)
    }
}

function leerAlmacenes (callback) {
    const fecha = moment().subtract(configuracion.dias, 'days').format('YYYYMMDD')
    const almacenes = []
    const sql = `
        SELECT ISNULL(a.logInstanc, 0) Instancia, o.WhsCode, o.WhsName
        FROM OWHS o
        LEFT JOIN AWHS a ON o.WhsCode = a.WhsCode
        LEFT JOIN BXP_SYNCAlmacenes bs ON o.WhsCode = bs.WhsCode
        WHERE
            (o.createDate >= '${fecha}' OR o.updateDate >= '${fecha}')
            AND ISNULL(a.logInstanc, 0) > ISNULL(bs.logInstance, -1)
    `
    console.log(sql)
    const query = new request(sql, function(err, rowCount) {
    })

    query.on('row', function(columns) {
        almacenes.push({
            instancia: columns[0].value,
            whsCode: columns[1].value,
            whsName: columns[2].value
        })
    })

    query.on('requestCompleted', function() {
        callback(null, almacenes)
    })

    state.conexion.execSql(query)
}

function buscarAlmacen (almacen, callback) {
    state.almacenService.find({query: { whsCode: almacen.whsCode }})
        .then(function (response) {
            const id = response.total > 0 ? response.data[0]._id : null
            callback(null, {id: id, data: almacen})
        })
        .catch(function (error) {
            callback(error)
        })
}

function upsertAlmacen (almacen, callback) {
    if (almacen.id) {
        state.almacenService.update(almacen.id, almacen.data)
            .then(function (response) {
                callback(null, almacen.data)
            })
            .catch(function (error) {
                callback(error)
            })
    } else {
        state.almacenService.create(almacen.data)
            .then(function (response) {
                callback(null, almacen.data)
            })
            .catch(function (error) {
                callback(error)
            })
    }
}

function marcarActualizado (almacen, callback) {
    const sql = `
        DELETE FROM BXP_SYNCAlmacenes WHERE WhsCode = '${almacen.whsCode}';
        INSERT INTO BXP_SYNCAlmacenes (WhsCode, WhsName, logInstance)
        VALUES (@whsCode, @whsName, @instancia);        
    `
    console.log(almacen)
    console.log(sql)
    const query = new request(sql, function(err, rowCount, rows) {
        if (err) {
            console.log(err)
            callback(err)
        } else {
            callback(null)
        }
    })

    query.addParameter('whsCode', TYPES.VarChar, almacen.whsCode)
    query.addParameter('whsName', TYPES.VarChar, almacen.whsName)
    query.addParameter('instancia', TYPES.Int, almacen.instancia)

    state.conexion.execSql(query)
}

function actualizarAlmacenes (almacenes, mainCallback) {
    const syncAlmacen = async.compose(marcarActualizado, upsertAlmacen, buscarAlmacen)

    async.eachSeries(almacenes, function (almacen, callback) {
        syncAlmacen(almacen, function (error, result) {
            if (error) callback(error)
            else callback(null)
        })
    }, function (err) {
        if (err) {
            mainCallback(err)
        } else {
            mainCallback(null)
        }
    })   
} // actualizarProductos

function complete (err, result) {
    if (err) {
        console.log(err.message)
    } else {
        console.log('Terminado')
    }
    state.conexion.close()
}

module.exports = function () {
    state.conexion = new connection(configuracion.connectionString)
    state.almacenService = sincronizacion.getService('almacenes')

    state.conexion.on('connect', main)
}