/*jslint node: true */
"use strict";

let _			= require( 'lodash' );
let fs          	= require( 'fs' );
let util		= require( 'util' );
let log			= require( './log.js' );
let desktopApp		= require( 'byteballcore/desktop_app.js' );

let m_sAppDataDir	= desktopApp.getAppDataDir();

let m_nProfilerExStart	= Date.now();
let m_oData		= {};
let m_oDefaultItem	= {
	count		: 0,
	time_first	: 0,
	time_last	: 0,
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
		m_oData[ sTag ] = _.cloneDeep( m_oDefaultItem );
		m_oData[ sTag ].time_first	= Date.now();
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
		m_oData[ sTag ].time_first	= Date.now();
		m_oData[ sTag ].time_start	= Date.now();
	}

	//	...
	m_oData[ sTag ].time_last	= Date.now();
	m_oData[ sTag ].count ++;
	m_oData[ sTag ].time_used	+= ( Date.now() - m_oData[ sTag ].time_start );
	if ( m_oData[ sTag ].time_used > 0 )
	{
		m_oData[ sTag ].qps		= ( ( m_oData[ sTag ].count * 1000 ) / m_oData[ sTag ].time_used ).toFixed( 2 );
	}
	else
	{
		m_oData[ sTag ].qps		= -1;
	}

}

function print()
{
//	https://nodejs.org/api/fs.html#fs_fs_createwritestream_path_options
	let m_oWriteStream	= fs.createWriteStream( m_sAppDataDir + '/profiler-ex.txt', { flags: 'w' } );

	//	...
	m_oWriteStream.write( "\n############################################################\r\n" );
	m_oWriteStream.write( Date().toString() + "\r\n\r\n" );
	m_oWriteStream.write( JSON.stringify( m_oData, null, 4 ) );
	m_oWriteStream.write( JSON.stringify( getSummary(), null, 4 ) );

	m_oWriteStream.end();


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


function getSummary()
{
	let oRet;
	let arrDataList;
	let nTotalTimeUsed;
	let nTotalExecutedCount;
	let nAverageQps;

	//	...
	arrDataList		= Object.values( m_oData );
	nTotalTimeUsed		= 0;
	nTotalExecutedCount	= 0;

	if ( Array.isArray( arrDataList ) && arrDataList.length > 0 )
	{
		nTotalTimeUsed		= arrDataList.reduce
		(
			function( nAccumulator, oCurrentValue )
			{
				return parseInt( nAccumulator ) + parseInt( oCurrentValue.time_used );
			},
			arrDataList[ 0 ].time_used
		);
		nTotalExecutedCount	= arrDataList.reduce
		(
			function( nAccumulator, oCurrentValue )
			{
				return parseInt( nAccumulator ) + parseInt( oCurrentValue.count );
			},
			arrDataList[ 0 ].count
		);
	}

	//	...
	if ( nTotalTimeUsed > 0 )
	{
		nAverageQps	= ( ( nTotalExecutedCount * 1000 ) / nTotalTimeUsed ).toFixed( 2 );
	}
	else
	{
		nAverageQps	= -1;
	}

	//	...
	return {
		"time_start"		: m_nProfilerExStart,
		"time_end"		: Date.now(),
		"time_elapsed"		: Date.now() - m_nProfilerExStart,
		"time_used"		: nTotalTimeUsed,
		"count_executed"	: nTotalExecutedCount,
		"average_qps"		: nAverageQps
	};
}


function printResults()
{
	log.consoleLog( JSON.stringify( getSummary(), null, 4 ) );
}




process.on
(
	'SIGINT',
	function()
	{
		log.consoleLog( "received sigint" );

		//	print();
		printResults();

		//	...
		process.exit();
	}
);




/**
 *	print profiler every 5 seconds
 */
setInterval
(
	function ()
	{
		print();
	},
	3 * 1000
);


exports.begin		= begin;	//	function(){};
exports.end		= end;		//	function(){};
exports.print		= print;
