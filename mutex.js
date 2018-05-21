/*jslint node: true */
"use strict";

let log		= require( './log.js' );
let _		= require( 'lodash' );
let singleton	= require( './enforce_singleton.js' );

/**
 *	member variables
 */
let m_arrQueuedJobs		= [];
let m_arrLockedKeyArrays	= [];



/**
 *	lock
 *	@public
 */
function lock( arrKeys, pfnProcedure, pfnNextProcedure )
{
	if ( _isAnyOfKeysLocked( arrKeys ) )
	{
		log.consoleLog( "queuing job held by keys", arrKeys );
		m_arrQueuedJobs.push
		(
			{
				arrKeys		: arrKeys,
				proc		: pfnProcedure,
				nextProc	: pfnNextProcedure,
				ts		: Date.now()
			}
		);
	}
	else
	{
		_execute( arrKeys, pfnProcedure, pfnNextProcedure );
	}
}

/**
 *	lock or skip
 *	@public
 */
function lockOrSkip( arrKeys, pfnProcedure, pfnNextProcedure )
{
	if ( _isAnyOfKeysLocked( arrKeys ) )
	{
		log.consoleLog( "skipping job held by keys", arrKeys );
		if ( pfnNextProcedure )
		{
			pfnNextProcedure();
		}
	}
	else
	{
		_execute( arrKeys, pfnProcedure, pfnNextProcedure );
	}
}

/**
 *	get job count
 *	@public
 */
function getCountOfQueuedJobs()
{
	return m_arrQueuedJobs.length;
}

/**
 *	get count of locks
 *	@public
 */
function getCountOfLocks()
{
	return m_arrLockedKeyArrays.length;
}







////////////////////////////////////////////////////////////////////////////////
//	Private
//


/**
 *	check if the {procedure} was locked with keys
 */
function _isAnyOfKeysLocked( arrKeys )
{
	let i;
	let j;
	let arrLockedKeys;

	for ( i = 0; i < m_arrLockedKeyArrays.length; i ++ )
	{
		arrLockedKeys	= m_arrLockedKeyArrays[ i ];
		for ( j = 0; j < arrLockedKeys.length; j ++ )
		{
			if ( -1 !== arrKeys.indexOf( arrLockedKeys[ j ] ) )
			{
				return true;
			}
		}
	}

	return false;
}

/**
 *	just release
 *	@param arrKeys
 *	@private
 */
function _release( arrKeys )
{
	let i;

	for ( i = 0; i < m_arrLockedKeyArrays.length; i ++ )
	{
		if ( _.isEqual( arrKeys, m_arrLockedKeyArrays[ i ] ) )
		{
			//
			//	remove the element from Array
			//
			m_arrLockedKeyArrays.splice( i, 1 );
			return true;
		}
	}

	return false;
}

/**
 *
 *	@param	arrKeys			array
 *	@param	pfnProcedure		function
 *	@param	pfnNextProcedure	function
 *	@private
 */
function _execute( arrKeys, pfnProcedure, pfnNextProcedure )
{
	let bLocked;

	//	...
	m_arrLockedKeyArrays.push( arrKeys );
	log.consoleLog( "lock acquired", arrKeys );

	//
	//	execute
	//
	bLocked	= true;
	pfnProcedure
	(
		function()
		{
			if ( ! bLocked )
			{
				throw Error( "double unlock?" );
			}

			//	...
			bLocked	= false;
			_release( arrKeys );

			//	...
			log.consoleLog( "lock released", arrKeys );

			//
			//	execute the next procedure
			//
			if ( pfnNextProcedure )
			{
				pfnNextProcedure.apply( pfnNextProcedure, arguments );
			}

			//	...
			_handleJobsInQueue();
		}
	);
}

/**
 * 	process the jobs in queue
 *	@private
 */
function _handleJobsInQueue()
{
	let i;
	let oJob;

	log.consoleLog( "_handleJobsInQueue, " + m_arrQueuedJobs.length + " items" );

	for ( i = 0; i < m_arrQueuedJobs.length; i ++ )
	{
		oJob	= m_arrQueuedJobs[ i ];
		if ( _isAnyOfKeysLocked( oJob.arrKeys ) )
		{
			//
			//	skip the locked items
			//
			continue;
		}

		//
		//	execute the job in queue
		//
		log.consoleLog( "_handleJobsInQueue, starting job held by keys", oJob.arrKeys );
		_execute( oJob.arrKeys, oJob.proc, oJob.nextProc );

		//
		//	WE'VE JUST REMOVED ONE ITEM
		//
		//	do it before _execute as _execute can trigger another job added,
		// 	another lock unlocked, another _handleJobsInQueue called
		//
		//	The splice() method
		//		changes the contents of an array by removing existing elements and/or adding new elements.
		//		https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/splice
		//
		m_arrQueuedJobs.splice( i, 1 );
		i --;
	}

	//	...
	log.consoleLog( "_handleJobsInQueue done " + m_arrQueuedJobs.length + " items" );
}

function _checkForDeadlocks()
{
	let i;
	let job;

	for ( i = 0; i < m_arrQueuedJobs.length; i ++ )
	{
		job	= m_arrQueuedJobs[ i ];
		if ( Date.now() - job.ts > 30 * 1000 )
		{
			throw Error
			(
				"possible deadlock on job " + require('util').inspect( job ) + ","
				+"\nproc:" + job.proc.toString() + " \n"
				+ "all jobs: " + require('util').inspect( m_arrQueuedJobs, { depth : null } )
			);
		}
	}
}






/**
 *	long running locks are normal in multisig scenarios
 *	setInterval(_checkForDeadlocks, 1000);
 */
setInterval
(
	function()
	{
		log.consoleLog
		(
			"queued jobs: " + JSON.stringify( m_arrQueuedJobs.map( function( job ){ return job.arrKeys; } ) )
			+ ", " +
			"locked keys: " + JSON.stringify( m_arrLockedKeyArrays )
		);
	},
	10000
);


/**
 *	exports
 */
exports.lock			= lock;
exports.lockOrSkip		= lockOrSkip;
exports.getCountOfQueuedJobs	= getCountOfQueuedJobs;
exports.getCountOfLocks		= getCountOfLocks;







/*
function test(key){
	var loc = "localvar"+key;
	lock(
		[key], 
		function(cb){
			log.consoleLog("doing "+key);
			setTimeout(function(){
				log.consoleLog("done "+key);
				cb("arg1", "arg2");
			}, 1000)
		},
		function(arg1, arg2){
			log.consoleLog("got "+arg1+", "+arg2+", loc="+loc);
		}
	);
}

test("key1");
test("key2");
*/
