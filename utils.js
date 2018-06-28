/*jslint node: true */
"use strict";


function version2int( version )
{
	var arr;

	//	...
	arr = version.split( '.' );
	return arr[ 0 ] * 10000 + arr[ 1 ] * 100 + arr[ 2 ] * 1;
}




/**
 *	exports
 */
exports.version2int		= version2int;
