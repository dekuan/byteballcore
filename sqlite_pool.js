/*jslint node: true */
"use strict";

var _				= require('lodash');
var _log			= require( './log.js' );
var _async			= require('async');
var _profiler_sql		= require('./profiler_sql.js');
var _sqlite_migrations		= require('./sqlite_migrations');

var EventEmitter		= require('events').EventEmitter;

var m_bCordova			= ( typeof window === 'object' && window.cordova );
var m_oSQLite3;
var m_path;
var cordovaSqlite;


if ( m_bCordova )
{
	// will error before deviceready
	//cordovaSqlite = window.cordova.require('cordova-sqlite-plugin.SQLite');
}
else
{
	m_oSQLite3	= require( 'sqlite3' );//.verbose();
	m_path	= require( './desktop_app.js' + '' ).getAppDataDir() + '/';
	_log.consoleLog( "path=" + m_path );
}


/**
 *	CSQLitePool
 */
function CSQLitePool( db_name, MAX_CONNECTIONS, bReadOnly )
{
	var m_cEventEmitter	= new EventEmitter();
	var m_bReady		= false;
	var m_arrConnections	= [];
	var m_arrQueue		= [];


	function openDb( cb )
	{
		if ( m_bCordova )
		{
			var db	= new cordovaSqlite(db_name);
			db.open( cb );
			return db;
		}
		else
		{
			return new m_oSQLite3.Database( m_path + db_name, bReadOnly ? m_oSQLite3.OPEN_READONLY : m_oSQLite3.OPEN_READWRITE, cb );
		}
	}

	function connect( handleConnection )
	{
		_log.consoleLog("opening new db connection");
		var db = openDb
		(
			function( err )
			{
				if ( err )
				{
					throw Error( err );
				}

				//	...
				_log.consoleLog("opened db");
				//	if (!m_bCordova)
				//		db.serialize();

				//	...
				connection.query
				(
					"PRAGMA foreign_keys = 1",
					function()
					{
						connection.query
						(
							"PRAGMA busy_timeout=30000",
							function()
							{
								connection.query
								(
									"PRAGMA journal_mode=WAL",
									function()
									{
										connection.query
										(
											"PRAGMA synchronous=FULL",
											function()
											{
												connection.query
												(
													"PRAGMA temp_store=MEMORY",
													function()
													{
														_sqlite_migrations.migrateDb
														(
															connection,
															function()
															{
																//
																//	finally
																//	callback ...
																//
																handleConnection
																(
																	connection
																);
															}
														);
													}
												);
											}
										);
									}
								);
							}
						);
					}
				);
			}
		);

		//
		//
		//
		var connection =
		{
			db	: db,
			bInUse	: true,

			release	: function()
			{
				//	_log.consoleLog("released connection");
				this.bInUse	= false;
				if ( m_arrQueue.length === 0 )
				{
					return;
				}

				//	...
				var connectionHandler	= m_arrQueue.shift();
				this.bInUse		= true;
				connectionHandler( this );
			},

			query : function()
			{
				var last_arg;
				var bHasCallback;
				var sql;
				var bSelect;
				var count_arguments_without_callback;
				var new_args;
				var self;
				var i;
				var start_ts;

				var sKeyString;

				if ( ! this.bInUse )
				{
					throw Error( "this connection was returned to the pool" );
				}

				//	...
				last_arg	= arguments[arguments.length - 1];
				bHasCallback	= (typeof last_arg === 'function');

				if ( ! bHasCallback )
				{
					// no callback
					last_arg = function(){};
				}

				//	...
				sql		= arguments[ 0 ];
				//_log.consoleLog("======= query: "+sql);
				bSelect		= !! sql.match( /^SELECT/i );
				count_arguments_without_callback	= bHasCallback ? ( arguments.length - 1 ) : arguments.length;
				new_args	= [];
				self		= this;

				for ( i = 0; i < count_arguments_without_callback; i ++ )	//	except the final callback
				{
					new_args.push( arguments[ i ] );
				}
				if ( count_arguments_without_callback === 1 )	//	no params
				{
					new_args.push( [] );
				}

				//	...
				expandArrayPlaceholders( new_args );


				//	...
				sKeyString = new_args.filter
				(
					function( a, i )
					{
						return ( i < new_args.length );
					}
				).join( ", " );
				_profiler_sql.begin( sKeyString );


				//	add callback with error handling
				new_args.push
				(
					function( err, result )
					{
						_profiler_sql.end( sKeyString );

						//	...
						var consumed_time;

						//	_log.consoleLog("query done: "+sql);
						if ( err )
						{
							console.error( "\nfailed query:", new_args );
							throw Error
							(
								err + "\n" + sql + "\n" + new_args[ 1 ].map
								(
									function( param )
									{
										if ( param === null )
											return 'null';

										if ( param === undefined )
											return 'undefined';

										return param;
									}
								).join(', ')
							);
						}

						//
						//	note that sqlite3 sets nonzero this.changes even when rows were matched but nothing actually changed (new values are same as old)
						//	this.changes appears to be correct for INSERTs despite the documentation states the opposite
						//
						if ( ! bSelect && ! m_bCordova )
						{
							result = { affectedRows : this.changes, insertId : this.lastID };
						}
						if ( bSelect && m_bCordova )
						{
							// note that on android, result.affectedRows is 1 even when inserted many rows
							result = result.rows || [];
						}

						//_log.consoleLog("changes="+this.changes+", affected="+result.affectedRows);

						consumed_time	= Date.now() - start_ts;
						if ( consumed_time > 25 )
						{
							_log.consoleLog
							(
								"long query took " + consumed_time + "ms:\n"
								+ new_args.filter
								(
									function( a, i )
									{
										return ( i < new_args.length - 1 );
									}
								).join( ", " ) + "\nload avg: " + require( 'os' ).loadavg().join( ', ' )
							);
						}

						//	...
						last_arg( result );
					}
				);

				//	...
				start_ts	= Date.now();
				if ( m_bCordova )
				{
					//	for Cordova only
					this.db.query.apply( this.db, new_args );
				}
				else
				{
					bSelect
						? this.db.all.apply( this.db, new_args )
						: this.db.run.apply( this.db, new_args );
				}
			},
			
			addQuery		: addQuery,
			escape			: escape,
			addTime			: addTime,
			getNow			: getNow,
			getUnixTimestamp	: getUnixTimestamp,
			getFromUnixTime		: getFromUnixTime,
			getRandom		: getRandom,
			getIgnore		: getIgnore,
			forceIndex		: forceIndex,
			dropTemporaryTable	: dropTemporaryTable
		};

		//	...
		m_arrConnections.push( connection );
	}

	//
	//	accumulate array of functions for _async.series()
	//	it applies both to individual connection and to pool
	//
	function addQuery( arr )
	{
		var self	= this;
		var query_args	= [];

		//	...
		for ( var i = 1; i < arguments.length; i++ )
		{
			//	except first, which is array
			query_args.push( arguments[ i ] );
		}

		//	...
		arr.push
		(
			function( callback )
			{
				//	add callback for _async.series() member tasks
				if ( typeof query_args[ query_args.length - 1 ] !== 'function' )
				{
					//	add callback
					query_args.push
					(
						function()
						{
							callback();
						}
					);
				}
				else
				{
					//
					//	the last parameter is an address of function
					//
					var f = query_args[ query_args.length - 1 ];
					query_args[ query_args.length - 1 ] = function()
					{
						//	add callback() call to the end of the function
						f.apply( f, arguments );
						callback();
					}
				}

				//	...
				self.query.apply( self, query_args );
			}
		);
	}

	function takeConnectionFromPool( handleConnection )
	{
		if ( ! m_bReady )
		{
			_log.consoleLog( "takeConnectionFromPool will wait for ready" );
			m_cEventEmitter.once
			(
				'ready',
				function()
				{
					_log.consoleLog( "db is now ready" );
					takeConnectionFromPool( handleConnection );
				}
			);
			return;
		}

		//
		//	first, try to find a free connection
		//
		for ( var i = 0; i < m_arrConnections.length; i++ )
		{
			if ( ! m_arrConnections[ i ].bInUse )
			{
				//	_log.consoleLog("reusing previously opened connection");
				m_arrConnections[ i ].bInUse	= true;
				return handleConnection( m_arrConnections[ i ] );
			}
		}

		//
		//	second, try to open a new connection
		//
		if ( m_arrConnections.length < MAX_CONNECTIONS )
		{
			return connect( handleConnection );
		}

		//
		//	third, queue it
		//	_log.consoleLog("queuing");
		//
		m_arrQueue.push( handleConnection );
	}


	function onDbReady()
	{
		if ( m_bCordova && ! cordovaSqlite )
		{
			cordovaSqlite = window.cordova.require( 'cordova-sqlite-plugin.SQLite' );
		}

		//	...
		m_bReady = true;

		//
		//	will be processed in
		//		function takeConnectionFromPool( handleConnection )
		//
		m_cEventEmitter.emit( 'ready' );
	}

	function getCountUsedConnections()
	{
		var count = 0;

		for ( var i = 0; i < m_arrConnections.length; i++ )
		{
			if ( m_arrConnections[ i ].bInUse )
			{
				count ++;
			}
		}

		//	...
		return count;
	}

	/**
	 *	takes a connection from the pool,
	 *	executes the single query on this connection, and immediately releases the connection
	 */
	function query()
	{
		//	_log.consoleLog(arguments[0]);
		var args	= arguments;

		//
		//	to execute SQL querying task
		//	is call the callback function immediately by passing in a picked database connection as parameter
		//
		takeConnectionFromPool
		(
			function( connection )
			{
				//
				//	with the picked database connection handle, we have the ability to query, insert, update and more
				//
				var last_arg		= args[ args.length - 1 ];
				var bHasCallback	= ( typeof last_arg === 'function' );

				if ( ! bHasCallback )
				{
					//	no callback
					last_arg = function(){};
				}

				var count_arguments_without_callback = bHasCallback
					? ( args.length - 1 )
					: args.length;
				var new_args = [];

				for ( var i = 0; i < count_arguments_without_callback; i ++ )
				{
					//	except callback
					new_args.push( args[ i ] );
				}

				//	add callback that releases the connection before calling the supplied callback
				new_args.push
				(
					function( rows )
					{
						connection.release();
						last_arg( rows );
					}
				);

				//
				//	now, we execute the SQL task
				//
				connection.query.apply
				(
					connection,
					new_args
				);
			}
		);
	}

	function close( cb )
	{
		if ( ! cb)
		{
			cb = function(){};
		}

		//	...
		m_bReady = false;
		if ( m_arrConnections.length === 0 )
		{
			return cb();
		}

		//	...
		m_arrConnections[ 0 ].db.close( cb );
		m_arrConnections.shift();
	}

	//	interval is string such as -8 SECOND
	function addTime( interval )
	{
		return "datetime('now', '" + interval + "')";
	}

	function getNow()
	{
		return "datetime('now')";
	}

	function getUnixTimestamp( date )
	{
		return "strftime('%s', " + date + ")";
	}

	function getFromUnixTime( ts )
	{
		return "datetime(" + ts + ", 'unixepoch')";
	}

	function getRandom()
	{
		return "RANDOM()";
	}

	function forceIndex( index )
	{
		return "INDEXED BY " + index;
	}

	function dropTemporaryTable( table )
	{
		return "DROP TABLE IF EXISTS " + table;
	}

	// note that IGNORE behaves differently from mysql.  In particular, if you insert and forget to specify a NOT NULL colum without DEFAULT value, 
	// sqlite will ignore while mysql will throw an error
	function getIgnore()
	{
		return "OR IGNORE";
	}

	function escape( str )
	{
		if ( typeof str === 'string' )
		{
			return "'" + str.replace( /'/g, "''" ) + "'";
		}
		else if ( Array.isArray( str ) )
		{
			return str.map( function( member ){ return escape( member ); } ).join( "," );
		}
		else
		{
			throw Error( "escape: unknown type " + ( typeof str ) );
		}
	}
	

	//
	//	...
	//
	createDatabaseIfNecessary( db_name, onDbReady );

	//	...
	var pool	= {};
	pool.query			= query;
	pool.addQuery			= addQuery;
	pool.takeConnectionFromPool	= takeConnectionFromPool;
	pool.getCountUsedConnections	= getCountUsedConnections;
	pool.close			= close;
	pool.escape			= escape;
	pool.addTime			= addTime;
	pool.getNow			= getNow;
	pool.getUnixTimestamp		= getUnixTimestamp;
	pool.getFromUnixTime		= getFromUnixTime;
	pool.getRandom			= getRandom;
	pool.getIgnore			= getIgnore;
	pool.forceIndex			= forceIndex;
	pool.dropTemporaryTable		= dropTemporaryTable;

	//	...
	return pool;
}








//
//	expands IN(?) into IN(?,?,?) and flattens parameter array
//	the function modifies first two memebers of the args array in place
//	will misbehave if there are ? in SQL comments
//
function expandArrayPlaceholders( args )
{
	var sql = args[0];
	var params = args[1];
	if (!Array.isArray(params) || params.length === 0)
		return;
	var assocLengthsOfArrayParams = {};
	for (var i=0; i<params.length; i++)
		if (Array.isArray(params[i])){
			if (params[i].length === 0)
				throw Error("empty array in query params");
			assocLengthsOfArrayParams[i] = params[i].length;
		}
	if (Object.keys(assocLengthsOfArrayParams).length === 0)
		return;
	var arrParts = sql.split('?');
	if (arrParts.length - 1 !== params.length)
		throw Error("wrong parameter count");
	var expanded_sql = "";
	for (var i=0; i<arrParts.length; i++){
		expanded_sql += arrParts[i];
		if (i === arrParts.length-1) // last part
			break;
		var len = assocLengthsOfArrayParams[i];
		if (len) // array
			expanded_sql += _.fill(Array(len), "?").join(",");
		else
			expanded_sql += "?";
	}
	var flattened_params = _.flatten(params);
	args[0] = expanded_sql;
	args[1] = flattened_params;
}


function getParentDirPath(){
	switch(window.cordova.platformId){
		case 'ios': 
			return window.cordova.file.applicationStorageDirectory + '/Library';
		case 'android': 
		default:
			return window.cordova.file.applicationStorageDirectory;
	}
}

function getDatabaseDirName(){
	switch(window.cordova.platformId){
		case 'ios': 
			return 'LocalDatabase';
		case 'android': 
		default:
			return 'databases';
	}
}

function getDatabaseDirPath()
{
	return getParentDirPath() + '/' + getDatabaseDirName();
}


function createDatabaseIfNecessary( db_name, onDbReady )
{
	_log.consoleLog( 'createDatabaseIfNecessary ' + db_name );
	var initial_db_filename = 'initial.' + db_name;

	//
	//	on mobile platforms,
	//	copy initial sqlite file from app root to data folder where we can open it for writing
	//
	if ( m_bCordova )
	{
		_log.consoleLog( "will wait for deviceready" );
		document.addEventListener
		(
			"deviceready",
			function onDeviceReady()
			{
				_log.consoleLog( "deviceready handler" );
				_log.consoleLog( "data dir: " + window.cordova.file.dataDirectory );
				_log.consoleLog( "app dir: " + window.cordova.file.applicationDirectory );

				window.requestFileSystem
				(
					LocalFileSystem.PERSISTENT,
					0,
					function onFileSystemSuccess( fs )
					{
						window.resolveLocalFileSystemURL
						(
							getDatabaseDirPath() + '/' + db_name,
							function( fileEntry )
							{
								_log.consoleLog( "database file already exists" );
								onDbReady();
							},
							function onSqliteNotInited( err )
							{
								//	file not found
								_log.consoleLog( "will copy initial database file" );
								window.resolveLocalFileSystemURL
								(
									window.cordova.file.applicationDirectory + "/www/" + initial_db_filename,
									function( fileEntry )
									{
										_log.consoleLog( "got initial db fileentry" );

										//	get parent dir
										window.resolveLocalFileSystemURL
										(
											getParentDirPath(),
											function( parentDirEntry )
											{
												_log.consoleLog( "resolved parent dir" );
												parentDirEntry.getDirectory
												(
													getDatabaseDirName(),
													{
														create	: true
													},
													function( dbDirEntry )
													{
														_log.consoleLog( "resolved db dir" );
														fileEntry.copyTo
														(
															dbDirEntry,
															db_name,
															function()
															{
																_log.consoleLog( "copied initial cordova database" );
																onDbReady();
															},
															function( err )
															{
																throw Error( "failed to copyTo: " + JSON.stringify( err ) );
															}
														);
													},
													function( err )
													{
														throw Error( "failed to getDirectory databases: " + JSON.stringify(err) );
													}
												);
											},
											function( err )
											{
												throw Error( "failed to resolveLocalFileSystemURL of parent dir: " + JSON.stringify( err ) );
											}
										);
									},
									function( err )
									{
										throw Error( "failed to getFile: " + JSON.stringify( err ) );
									}
								);
							}
						);
					},
					function onFailure( err )
					{
						throw Error("failed to requestFileSystem: "+err);
					}
				);
			},
			false
		);
	}
	else
	{
		//
		//	copy initial db to app folder
		//
		//	Using fs.stat() to check for the existence of a file
		//
		var fs	= require( 'fs' + '' );
		fs.stat
		(
			m_path + db_name,
			function( err, stats )
			{
				_log.consoleLog( "stat " + err );

				if ( ! err )
				{
					//	already exists
					return onDbReady();
				}

				//	...
				_log.consoleLog( "will copy initial db" );

				var mode	= parseInt( '700', 8 );
				var parent_dir	= require( 'path' + '' ).dirname( m_path );

				fs.mkdir
				(
					parent_dir,
					mode,
					function( err )
					{
						_log.consoleLog( 'mkdir ' + parent_dir + ': ' + err );
						fs.mkdir
						(
							m_path,
							mode,
							function( err )
							{
								_log.consoleLog('mkdir '+m_path+': '+err);
								fs.createReadStream
								(
									__dirname + '/' + initial_db_filename
								).pipe
								(
									fs.createWriteStream( m_path + db_name ) )
								.on
								(
									'finish',
									onDbReady
								);
							}
						);
					}
				);
			}
		);
	}


}




/**
 *	exports
 */
module.exports		= CSQLitePool;

