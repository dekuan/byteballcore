/*jslint node: true */
"use strict";


/**
 *	print log on console with current date time
 */
function consoleLog()
{
	var oDate;
	var sDateText;
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
	oDate		= new Date();
	sDateText	= "[" + oDate.getTime() + "]";

	//
	//	insert date text the the beginning of the array
	//
	arrArgs.unshift( sDateText );

	//	...
	console.log.apply( this, arrArgs );
}



/**
 *
 */
exports.consoleLog	= consoleLog;