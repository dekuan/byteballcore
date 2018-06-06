/*jslint node: true */
"use strict";

var _				= require( 'lodash' );
var fs          		= require( 'fs' );
var log				= require( './log.js' );
var desktopApp			= require( 'byteballcore/desktop_app.js' );

var m_sAppDataDir		= desktopApp.getAppDataDir();

var m_nProcessedUnitCount	= 0;
var m_nProfilerExStart		= Date.now();
var m_oData			= {};
var m_oDefaultItem		= {
	count		: 0,
	time_first	: 0,
	time_last	: 0,
	time_start	: 0,
	time_used_total	: 0,
	time_used_avg	: 0,
	qps		: 0
};


//	...
function begin( sTag )
{
	//	...
	sTag	= String( sTag );

	if ( 0 === sTag.length )
	{
		throw Error( "profiler, sql, invalid tag " );
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
		throw Error( "profiler, sql, invalid tag " );
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
	m_oData[ sTag ].time_used_total	+= ( Date.now() - m_oData[ sTag ].time_start );
	m_oData[ sTag ].time_used_avg	= ( m_oData[ sTag ].time_used_total / m_oData[ sTag ].count ).toFixed( 2 );
	if ( m_oData[ sTag ].time_used_total > 0 )
	{
		m_oData[ sTag ].qps		= ( ( m_oData[ sTag ].count * 1000 ) / m_oData[ sTag ].time_used_total ).toFixed( 2 );
	}
	else
	{
		m_oData[ sTag ].qps		= -1;
	}

}

function increase()
{
	m_nProcessedUnitCount ++;
}


function print()
{
	try
	{
		//	https://nodejs.org/api/fs.html#fs_fs_createwritestream_path_options
		var m_oWriteStream	= fs.createWriteStream( m_sAppDataDir + '/profiler-sql.txt', { flags: 'w' } );

		//	...
		m_oWriteStream.write( "\n############################################################\r\n" );
		m_oWriteStream.write( Date().toString() + "\r\n\r\n" );
		m_oWriteStream.write( JSON.stringify( getSortedDataObject( m_oData ), null, 4 ) );
		m_oWriteStream.write( JSON.stringify( getSummary(), null, 4 ) );

		m_oWriteStream.end();
	}
	catch ( e )
	{

	}
}



function getSortedDataObject( oData )
{
	var arrDataList	= [];
	var sKey;
	var oNewObject;

	for ( sKey in oData )
	{
		oNewObject		= oData[ sKey ];
		oNewObject.key		= sKey;
		oNewObject.count	= parseFloat( oNewObject.count );

		arrDataList.push( oNewObject );
	}

	arrDataList.sort
	(
		function( a, b )
		{
			return b.time_used_avg - a.time_used_avg;
		}
	);

	return arrDataList.filter
	(
		function ( oObject )
		{
			return oObject.count > 0;
		}
	)
	.reduce
	(
		function( oAcc, oCurrent )
		{
			oAcc[ oCurrent.key ]	= oCurrent;
			delete oAcc[ oCurrent.key ].key;

			//	...
			return oAcc;
		},
		{}
	);
}


function getSummary()
{
	var arrDataList;
	var nTotalTimeUsed;
	var nTotalTimeUnitProcess;
	var nTotalExecutedCount;
	var nAverageNormalQps;
	var nAverageUnitProcessQps;

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
				return parseInt( nAccumulator ) + parseInt( oCurrentValue.time_used_total );
			},
			arrDataList[ 0 ].time_used_total
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
	nTotalTimeUnitProcess	= 0;

	if ( nTotalTimeUsed > 0 )
	{
		nAverageNormalQps	= ( ( nTotalExecutedCount * 1000 ) / nTotalTimeUsed ).toFixed( 2 );

		//
		//	...
		//
		nTotalTimeUnitProcess	+= m_oData.hasOwnProperty( '#updateMainChain' ) ? m_oData[ '#updateMainChain' ].time_used_total : 0;
		nTotalTimeUnitProcess	+= m_oData.hasOwnProperty( '#validate' ) ? m_oData[ '#validate' ].time_used_total : 0;
		nTotalTimeUnitProcess	+= m_oData.hasOwnProperty( '#saveJoint' ) ? m_oData[ '#saveJoint' ].time_used_total : 0;
		nAverageUnitProcessQps	= ( ( m_nProcessedUnitCount * 1000 ) / nTotalTimeUnitProcess ).toFixed( 2 );
	}
	else
	{
		nAverageNormalQps	= -1;
		nAverageUnitProcessQps	= -1;
	}

	//	...
	return {
		"time_start"				: m_nProfilerExStart,
		"time_end"				: Date.now(),
		"time_elapsed"				: Date.now() - m_nProfilerExStart,
		"time_used_total"			: nTotalTimeUsed,
		"count_executed"			: nTotalExecutedCount,
		"average_normal_qps"			: nAverageNormalQps,
		"units"	: {
			"time_used_total_processed_units"	: nTotalTimeUnitProcess,
			"count_processed_units"			: m_nProcessedUnitCount,
			"average_unit_process_qps"		: nAverageUnitProcessQps
		}
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
exports.increase	= increase;	//	function(){};
exports.print		= print;
