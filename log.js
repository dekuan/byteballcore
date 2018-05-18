/*jslint node: true */
"use strict";


function consoleLog()
{
	var sText;
	var arrArgs;

	//	...
	arrArgs	= Array.isArray( arguments ) ? Array.from( arguments ) : [];

	sText	= ( new Date() ).toString() + "\t";
	sText += arrArgs.join( ", " );

	console.log( sText );
}



/**
 *
 */
exports.consoleLog	= consoleLog;