/*jslint node: true */
"use strict";

let log		= require( './log.js' );
let _		= require( 'lodash' );
require( './enforce_singleton.js' );


let m_arrQueuedJobs		= [];
let m_arrLockedKeyArrays	= [];



/**
 *	lock
 */
function lock( arrKeys, proc, next_proc )
{
	if ( _isAnyOfKeysLocked( arrKeys ) )
	{
		log.consoleLog( "queuing job held by keys", arrKeys );
		m_arrQueuedJobs.push
		(
			{
				arrKeys		: arrKeys,
				proc		: proc,
				next_proc	: next_proc,
				ts		: Date.now()
			}
		);
	}
	else
	{
		_execute( arrKeys, proc, next_proc );
	}
}

/**
 *	lock or skip
 */
function lockOrSkip( arrKeys, proc, next_proc )
{
	if ( _isAnyOfKeysLocked( arrKeys ) )
	{
		log.consoleLog( "skipping job held by keys", arrKeys );
		if ( next_proc )
		{
			next_proc();
		}
	}
	else
	{
		_execute( arrKeys, proc, next_proc );
	}
}

/**
 *	get job count
 */
function getCountOfQueuedJobs()
{
	return m_arrQueuedJobs.length;
}

/**
 *	get count of locks
 */
function getCountOfLocks()
{
	return m_arrLockedKeyArrays.length;
}










function _isAnyOfKeysLocked( arrKeys )
{
	let i;
	let j;
	let arrLockedKeys;

	for ( i = 0; i < m_arrLockedKeyArrays.length; i++ )
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


function _release( arrKeys )
{
	let i;

	for ( i = 0; i < m_arrLockedKeyArrays.length; i ++ )
	{
		if ( _.isEqual( arrKeys, m_arrLockedKeyArrays[ i ] ) )
		{
			//	remove the element from Array
			m_arrLockedKeyArrays.splice( i, 1 );
			return;
		}
	}
}

function _execute( arrKeys, proc, next_proc )
{
	let bLocked = true;

	//	...
	m_arrLockedKeyArrays.push( arrKeys );
	log.consoleLog( "lock acquired", arrKeys );

	//	...
	proc
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

			if ( next_proc )
			{
				next_proc.apply( next_proc, arguments );
			}

			//	...
			_handleQueue();
		}
	);
}

function _handleQueue()
{
	let i;
	let job;

	log.consoleLog( "_handleQueue " + m_arrQueuedJobs.length + " items" );

	for ( i = 0; i < m_arrQueuedJobs.length; i++ )
	{
		job	= m_arrQueuedJobs[ i ];
		if ( _isAnyOfKeysLocked( job.arrKeys ) )
		{
			continue;
		}

		//
		//	The splice() method
		// 	changes the contents of an array by removing existing elements and/or adding new elements.
		//	https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/splice
		//
		//	do it before _execute as _execute can trigger another job added, another lock unlocked, another _handleQueue called
		//
		m_arrQueuedJobs.splice( i, 1 );
		log.consoleLog( "starting job held by keys", job.arrKeys );
		_execute( job.arrKeys, job.proc, job.next_proc );

		//	we've just removed one item
		i --;
	}

	log.consoleLog( "_handleQueue done " + m_arrQueuedJobs.length + " items" );
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
