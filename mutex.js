/*jslint node: true */
"use strict";

var _	= require('lodash');
require('./enforce_singleton.js');


var m_arrQueuedJobs		= [];
var m_arrLockedKeyArrays	= [];


/**
 *	lock
 */
function lock( arrKeys, procedure, nextProcedure )
{
	if ( _isAnyOfKeysLocked( arrKeys ) )
	{
		console.log( "queuing job held by keys", arrKeys );
		m_arrQueuedJobs.push
		(
			{
				arrKeys		: arrKeys,
				procedure	: procedure,
				nextProcedure	: nextProcedure,
				ts		: Date.now()
			}
		);
	}
	else
	{
		_execute( arrKeys, procedure, nextProcedure );
	}
}

function lockOrSkip( arrKeys, procedure, nextProcedure )
{
	if ( _isAnyOfKeysLocked( arrKeys ) )
	{
		console.log( "skipping job held by keys", arrKeys );
		if ( nextProcedure )
		{
			nextProcedure();
		}
	}
	else
	{
		_execute( arrKeys, procedure, nextProcedure );
	}
}

function getCountOfQueuedJobs()
{
	return m_arrQueuedJobs.length;
}

function getCountOfLocks()
{
	return m_arrLockedKeyArrays.length;
}






////////////////////////////////////////////////////////////////////////////////
//	Private
//

function _isAnyOfKeysLocked( arrKeys )
{
	let i;
	let j;
	let arrLockedKeys;

	for ( i = 0; i < m_arrLockedKeyArrays.length; i ++ )
	{
		arrLockedKeys = m_arrLockedKeyArrays[i];
		for ( j = 0; j < arrLockedKeys.length; j ++ )
		{
			if ( arrKeys.indexOf( arrLockedKeys[ j ] ) !== -1 )
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
			m_arrLockedKeyArrays.splice( i, 1 );
			return;
		}
	}
}

function _execute( arrKeys, procedure, nextProcedure )
{
	let bLocked;

	m_arrLockedKeyArrays.push( arrKeys );
	console.log( "lock acquired", arrKeys );

	//	...
	bLocked = true;
	procedure
	(
		function()
		{
			if ( ! bLocked )
			{
				throw Error( "double unlock?" );
			}

			bLocked = false;
			_release( arrKeys );
			console.log( "lock released", arrKeys );

			if ( nextProcedure )
			{
				nextProcedure.apply( nextProcedure, arguments );
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

	//	...
	console.log("_handleQueue "+m_arrQueuedJobs.length+" items");
	for ( i = 0; i < m_arrQueuedJobs.length; i ++ )
	{
		job	= m_arrQueuedJobs[ i ];
		if ( _isAnyOfKeysLocked( job.arrKeys ) )
		{
			continue;
		}

		//	do it before _execute as _execute can trigger another job added, another lock unlocked, another _handleQueue called
		m_arrQueuedJobs.splice( i, 1 );
		console.log( "starting job held by keys", job.arrKeys );

		//	...
		_execute( job.arrKeys, job.procedure, job.nextProcedure );

		//	we've just removed one item
		i--;
	}

	console.log( "_handleQueue done " + m_arrQueuedJobs.length + " items" );
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
				"possible deadlock on job " + require('util').inspect(job) + ",\n"
				+ "procedure:" + job.procedure.toString() + " \n"
				+ "all jobs: " + require('util').inspect( m_arrQueuedJobs, { depth: null } )
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
		console.log
		(
			"queued jobs: " + JSON.stringify
			(
				m_arrQueuedJobs.map
				(
					function( job )
					{
						return job.arrKeys;
					}
				)
			) + ", locked keys: " + JSON.stringify( m_arrLockedKeyArrays )
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
			console.log("doing "+key);
			setTimeout(function(){
				console.log("done "+key);
				cb("arg1", "arg2");
			}, 1000)
		},
		function(arg1, arg2){
			console.log("got "+arg1+", "+arg2+", loc="+loc);
		}
	);
}

test("key1");
test("key2");
*/
