'use strict';


function consoleLog()
{
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
	sText	= ( new Date() ).toString() + "\t";
	sText += arrArgs.join( ", " );

	console.log( sText );
}


function test_log()
{
	consoleLog( "111111" );
}


test_log();