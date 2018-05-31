/*jslint node: true */
"use strict";

var log			= require( './log.js' );

var m_nCount		= 0;
var m_oTimes		= {};
var m_nStartTs		= 0;

var m_oTimer		= {};
var m_oTimerResult	= {};
var m_nProfilerStartTs	= Date.now();



function mark_start( tag, id )
{
	return;

	if ( ! id )
	{
		id = 0;
	}
	if ( ! m_oTimer[ tag ] )
	{
		m_oTimer[ tag ] = {};
	}
	if ( m_oTimer[ tag ][ id ] )
	{
		throw Error( "multiple start marks for " + tag + "[" + id + "]" );
	}

	//	...
	m_oTimer[ tag ][ id ]	= Date.now();
}

function mark_end( tag, id )
{
	return;

	if ( ! m_oTimer[ tag ] )
	{
		return;
	}
	if ( ! id )
	{
		id = 0;
	}
	if ( ! m_oTimerResult[ tag ] )
	{
		m_oTimerResult[ tag ] = [];
	}

	//	...
	m_oTimerResult[ tag ].push( Date.now() - m_oTimer[ tag ][ id ] );
	m_oTimer[ tag ][ id ]	= 0;
}

function print_results()
{
	var tag;
	var results;
	var sum;
	var max;
	var min;
	var i;
	var v;

	log.consoleLog( "\nBenchmarking results:" );

	for ( tag in m_oTimerResult )
	{
		results	= m_oTimerResult[ tag ];
		sum	= 0;
		max	= 0;
		min	= 999999999999;

		for ( i = 0; i < results.length; i++ )
		{
			v	= results[i];
			sum	+= v;

			if ( v > max )
			{
				max = v;
			}
			if ( v < min )
			{
				min = v;
			}
		}

		log.consoleLog
		(
			tag.padding( 50 ) + ": "
			+ "avg:" + Math.round( sum / results.length ).toString().padding( 8 )
			+ "max:" + Math.round( max ).toString().padding( 8 )
			+ "min:" + Math.round( min ).toString().padding( 8 )
			+ "records:" + results.length
		);
	}

	//	...
	log.consoleLog
	(
		"\n\nStart time: " + m_nProfilerStartTs
		+ ", End time: " + Date.now()
		+ " Elapsed ms:" + ( Date.now() - m_nProfilerStartTs )
	);
}






function start()
{
	if ( m_nStartTs )
	{
		throw Error( "profiler already started" );
	}

	//	...
	m_nStartTs = Date.now();
}

function stop( tag )
{
	if ( ! m_nStartTs )
	{
		throw Error( "profiler not started" );
	}
	if ( ! m_oTimes[ tag ] )
	{
		m_oTimes[ tag ] = 0;
	}

	//	...
	m_oTimes[ tag ] += ( Date.now() - m_nStartTs );
	m_nStartTs = 0;
}

function print()
{
	var total	= 0;
	var tag;

	//	...
	log.consoleLog( "\nProfiling results:" );

	for ( tag in m_oTimes )
	{
		total += m_oTimes[ tag ];
	}

	for ( tag in m_oTimes )
	{
		log.consoleLog
		(
			pad_right( tag + ": ", 33 ) +
			pad_left( m_oTimes[ tag ], 5 ) + ', ' +
			pad_left( ( m_oTimes[ tag ] / m_nCount ).toFixed( 2 ), 5 ) + ' per unit, ' +
			pad_left( ( 100 * m_oTimes[ tag ] / total ).toFixed( 2 ), 5 ) + '%'
		);
	}

	//	...
	log.consoleLog( 'total: ' + total );
	log.consoleLog( ( total / m_nCount ) + ' per unit' );
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

function increment()
{
	m_nCount ++;
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
        var val = this.valueOf();
        if ( Math.abs( n ) <= val.length )
        {
                return val;
        }

        var m	= Math.max( ( Math.abs( n ) - this.length ) || 0, 0 );
        var pad	= Array( m + 1 ).join( String( c || ' ' ).charAt( 0 ) );
//      var pad = String(c || ' ').charAt(0).repeat(Math.abs(n) - this.length);
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
	5000
);




var clog = log.consoleLog;
//log.consoleLog = function(){};

//exports.start = start;
//exports.stop = stop;
//exports.increment = increment;
exports.print		= print;
exports.mark_start	= mark_start;
exports.mark_end	= mark_end;


exports.start		= function(){};
exports.stop		= function(){};
exports.increment	= function(){};
//exports.print		= function(){};
