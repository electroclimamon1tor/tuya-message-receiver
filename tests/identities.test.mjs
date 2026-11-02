import assert from 'node:assert/strict';
import test from 'node:test';
import {DatabaseSync} from 'node:sqlite';
import {guardarReporte} from '../worker.js';

function entorno() {
  const sqlite=new DatabaseSync(':memory:');
  sqlite.exec(`CREATE TABLE termometros(id TEXT PRIMARY KEY,sensor_id TEXT,alias TEXT);
    CREATE TABLE termometro_identidades(termometro_id TEXT,sensor_id TEXT PRIMARY KEY,inicio INTEGER,fin INTEGER);
    CREATE TABLE mediciones(id INTEGER PRIMARY KEY,sensor_id TEXT,nombre TEXT,temperatura_externa REAL,temperatura_ambiente REAL,humedad REAL,bateria TEXT,online INTEGER,fecha INTEGER);
    CREATE TABLE conectividad(id INTEGER PRIMARY KEY,sensor_id TEXT,nombre TEXT,estado TEXT,fecha INTEGER);
    INSERT INTO termometros VALUES ('fisico-2','id-nuevo-fixture','Termómetro 2');
    INSERT INTO termometro_identidades VALUES ('fisico-2','id-viejo-fixture',0,1000),('fisico-2','id-nuevo-fixture',1000,NULL);`);
  let beforeInsert=()=>{};
  function prepare(sql,args=[]) {
    return {bind(...values){return prepare(sql,values);},
      async first(){return sqlite.prepare(sql).get(...args)??null;},
      async run(){beforeInsert(sql);const r=sqlite.prepare(sql).run(...args);return {meta:{changes:Number(r.changes)}};}};
  }
  return {sqlite,env:{DB:{prepare}},intercept(fn){beforeInsert=fn;}};
}
function mensaje(id,fecha=2000) {
  return {bizCode:'devicePropertyMessage',ts:fecha,bizData:{devId:id,properties:[
    {code:'temp_current',value:215,time:fecha},{code:'temp_current_external',value:123,time:fecha},
    {code:'humidity_value',value:55,time:fecha},{code:'battery_state',value:'high',time:fecha}
  ]}};
}

test('receiver: acepta únicamente identidad activa, sin depender del nombre o IDs fijos',async()=>{
  const {env,sqlite}=entorno();
  assert.equal(await guardarReporte(env,mensaje('id-nuevo-fixture')),1);
  assert.equal(await guardarReporte(env,mensaje('id-viejo-fixture')),0);
  assert.equal(await guardarReporte(env,mensaje('desconocido')),0);
  const fila=sqlite.prepare('SELECT * FROM mediciones').get();
  assert.equal(fila.sensor_id,'id-nuevo-fixture');assert.equal(fila.nombre,'Termómetro 2');
  assert.equal(fila.temperatura_ambiente,21.5);assert.equal(fila.temperatura_externa,12.3);
  assert.equal(fila.humedad,55);assert.equal(fila.bateria,'high');
});

test('receiver: fecha válida y deduplicación se comprueban al guardar',async()=>{
  const {env,sqlite}=entorno();
  assert.equal(await guardarReporte(env,mensaje('id-nuevo-fixture',999)),0);
  assert.equal(await guardarReporte(env,mensaje('id-nuevo-fixture',1000)),1);
  assert.equal(await guardarReporte(env,mensaje('id-nuevo-fixture',1000)),0);
  assert.equal(sqlite.prepare('SELECT COUNT(*) n FROM mediciones').get().n,1);
});

test('receiver: revinculación concurrente impide escrituras de una identidad que dejó de estar activa',async()=>{
  const {env,sqlite,intercept}=entorno();
  intercept(()=>sqlite.exec("UPDATE termometro_identidades SET fin=1500 WHERE sensor_id='id-nuevo-fixture'"));
  assert.equal(await guardarReporte(env,mensaje('id-nuevo-fixture')),0);
  assert.equal(sqlite.prepare('SELECT COUNT(*) n FROM mediciones').get().n,0);
});

test('receiver: un termómetro futuro registrado en D1 funciona sin editar el worker, incluida conectividad',async()=>{
  const {env,sqlite}=entorno();
  sqlite.exec("INSERT INTO termometros VALUES ('fisico-3','id-futuro-fixture','Termómetro 3'); INSERT INTO termometro_identidades VALUES ('fisico-3','id-futuro-fixture',1000,NULL)");
  assert.equal(await guardarReporte(env,mensaje('id-futuro-fixture')),1);
  assert.equal(await guardarReporte(env,{bizCode:'deviceOffline',bizData:{devId:'id-futuro-fixture',time:2100}}),1);
  assert.equal(await guardarReporte(env,{bizCode:'deviceOnline',bizData:{devId:'desconocido',time:2200}}),0);
  assert.equal(sqlite.prepare('SELECT sensor_id FROM conectividad').get().sensor_id,'id-futuro-fixture');
});
