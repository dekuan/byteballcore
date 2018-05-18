/*jslint node: true */
"use strict";


/**
 *	print log on console with current date time
 */
function consoleLog()
{
	var oDate;
	var sText;
	var arrArgs;

	//	...
	if ( "[object Arguments]" === Object.prototype.toString.call( arguments ) )
	{
		arrArgs	= Array.from( arguments );
	}
	else
	{
		arrArgs	= [];
	}

	//	...
	oDate	= new Date();
	sText	= "[" + oDate.toLocaleString() + "/" + oDate.getTime() + "]\t";
	sText += arrArgs.join( ", " );

	console.log( sText );
}



/**
 *
 */
exports.consoleLog	= consoleLog;