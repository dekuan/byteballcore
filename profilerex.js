/*jslint node: true */
"use strict";

let _			= require( 'lodash' );
var fs          	= require( 'fs' );
var util		= require( 'util' );
let log			= require( './log.js' );
var desktopApp		= require( 'byteballcore/desktop_app.js' );

var m_sAppDataDir	= desktopApp.getAppDataDir();
let m_oWriteStream	= fs.createWriteStream( m_sAppDataDir + '/profiler-ex.txt' );

let m_oData		= {};
let m_oDefaultItem	= {
	count		: 0,
	time_start	: 0,
	time_used	: 0,
	qps		: 0
};


//	...
function begin( sTag )
{
	//	...
	sTag	= String( sTag );

	if ( 0 === sTag.length )
	{
		throw Error( "profiler, ex, invalid tag " );
	}

	if ( ! m_oData.hasOwnProperty( sTag ) )
	{
		m_oData[ sTag ]	= _.cloneDeep( m_oDefaultItem );
	}

	//	...
	m_oData[ sTag ].time_start	= Date.now();
}

function end( sTag )
{
	//	...
	sTag	= String( sTag );

	if ( 0 === sTag.length )
	{
		throw Error( "profiler, ex, invalid tag " );
	}

	if ( ! m_oData.hasOwnProperty( sTag ) )
	{
		m_oData[ sTag ]	= _.cloneDeep( m_oDefaultItem );
		m_oData[ sTag ].time_start	= Date.now();
	}

	//	...
	m_oData[ sTag ].count ++;
	m_oData[ sTag ].time_used	+= ( Date.now() - m_oData[ sTag ].time_start );
	m_oData[ sTag ].qps		= ( m_oData[ sTag ].time_used / m_oData[ sTag ].count ).toFixed( 2 );
}

function print()
{
	m_oWriteStream.write( "############################################################\r\n" );
	m_oWriteStream.write( Date().toString() + "\r\n\r\n" );
	m_oWriteStream.write( JSON.stringify( m_oData, null, 4 ) );

	//
	// log.consoleLog( "############################################################" );
	// log.consoleLog( "############################################################" );
	// log.consoleLog( m_oData );
	// log.consoleLog( "############################################################" );
	// log.consoleLog( "############################################################" );
	//
	//
	// let total	= 0;
	// let tag;
	//
	//
	//
	// //	...
	// log.consoleLog( "\nProfiling results:" );
	//
	// for ( tag in m_oTimes )
	// {
	// 	total += m_oTimes[ tag ];
	// }
	//
	// for ( tag in m_oTimes )
	// {
	// 	log.consoleLog
	// 	(
	// 		pad_right( tag + ": ", 33 ) +
	// 		pad_left( m_oTimes[ tag ], 5 ) + ', ' +
	// 		pad_left( ( m_oTimes[ tag ] / m_nCount ).toFixed( 2 ), 5 ) + ' per unit, ' +
	// 		pad_left( ( 100 * m_oTimes[ tag ] / total ).toFixed( 2 ), 5 ) + '%'
	// 	);
	// }
	//
	// //	...
	// log.consoleLog( 'total: ' + total );
	// log.consoleLog( ( total / m_nCount ) + ' per unit' );
}



function pad_right( str, len )
{
	if ( str.length >= len )
		return str;

	//	...
	return str + ' '.repeat( len - str.length );
}

function pad_left( str, len )
{
	//	...
	str = str+'';

	if ( str.length >= len )
	{
		return str;
	}

	return ' '.repeat( len - str.length ) + str;
}



process.on
(
	'SIGINT',
	function()
	{
		log.consoleLog	= clog;
		log.consoleLog( "received sigint" );
		//print();
		print_results();
		process.exit();
	}
);


String.prototype.padding = function( n, c )
{
	let val = this.valueOf();
	if ( Math.abs( n ) <= val.length )
	{
		return val;
	}

	let m	= Math.max( ( Math.abs( n ) - this.length ) || 0, 0 );
	let pad	= Array( m + 1 ).join( String( c || ' ' ).charAt( 0 ) );
//      let pad = String(c || ' ').charAt(0).repeat(Math.abs(n) - this.length);
	return ( n < 0 ) ? pad + val : val + pad;
//      return ( n < 0 ) ? val + pad : pad + val;
};



/**
 *	print profiler every 5 seconds
 */
setInterval
(
	function ()
	{
		print();
	},
	10 * 1000
);


exports.begin		= begin;	//	function(){};
exports.end		= end;		//	function(){};
exports.print		= print;
