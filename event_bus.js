/*jslint node: true */
"use strict";

var singleton		= require( './enforce_singleton.js' );
var EventEmitter	= require( 'events' ).EventEmitter;

//	...
var eventEmitter	= new EventEmitter();



/**
 *	set max listeners
 */
eventEmitter.setMaxListeners( 20 );



/**
 *	exports
 */
module.exports = eventEmitter;
