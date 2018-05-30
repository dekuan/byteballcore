/*jslint node: true */
"use strict";

var _			= require('lodash');
var log			= require( './log.js' );
var async		= require('async');
var sqlite_migrations	= require('./sqlite_migrations');
var EventEmitter	= require('events').EventEmitter;

var bCordova		= ( typeof window === 'object' && window.cordova );
var sqlite3;
var path;
var cordovaSqlite;


if ( bCordova )
{
	// will error before deviceready
	//cordovaSqlite = window.cordova.require('cordova-sqlite-plugin.SQLite');
}
else
{
	sqlite3	= require( 'sqlite3' );//.verbose();
	path	= require( './desktop_app.js' + '' ).getAppDataDir() + '/';
	log.consoleLog( "path=" + path );
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
		if ( bCordova )
		{
			var db	= new cordovaSqlite(db_name);
			db.open( cb );
			return db;
		}
		else
		{
			return new sqlite3.Database( path + db_name, bReadOnly ? sqlite3.OPEN_READONLY : sqlite3.OPEN_READWRITE, cb );
		}
	}

	function connect( handleConnection )
	{
		log.consoleLog("opening new db connection");
		var db = openDb
		(
			function( err )
			{
				if ( err )
				{
					throw Error( err );
				}

				//	...
				log.consoleLog("opened db");
				//	if (!bCordova)
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
														sqlite_migrations.migrateDb
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
				//	log.consoleLog("released connection");
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
				if ( ! this.bInUse )
				{
					throw Error( "this connection was returned to the pool" );
				}

				//	...
				var last_arg		= arguments[arguments.length - 1];
				var bHasCallback	= (typeof last_arg === 'function');
				if ( ! bHasCallback )
				{
					// no callback
					last_arg = function(){};
				}

				//	...
				var sql = arguments[0];
				//log.consoleLog("======= query: "+sql);
				var bSelect = !! sql.match( /^SELECT/i );
				var count_arguments_without_callback = bHasCallback ? (arguments.length-1) : arguments.length;
				var new_args = [];
				var self = this;

				for (var i=0; i<count_arguments_without_callback; i++) // except the final callback
					new_args.push(arguments[i]);

				if (count_arguments_without_callback === 1) // no params
					new_args.push([]);

				expandArrayPlaceholders( new_args );

				// add callback with error handling
				new_args.push
				(
					function( err, result )
					{
						//	log.consoleLog("query done: "+sql);
						if ( err )
						{
							console.error("\nfailed query:", new_args);
							throw Error(err+"\n"+sql+"\n"+new_args[1].map(function(param){ if (param === null) return 'null'; if (param === undefined) return 'undefined'; return param;}).join(', '));
						}

						//
						//	note that sqlite3 sets nonzero this.changes even when rows were matched but nothing actually changed (new values are same as old)
						//	this.changes appears to be correct for INSERTs despite the documentation states the opposite
						//
						if ( ! bSelect && ! bCordova )
						{
							result = { affectedRows : this.changes, insertId : this.lastID };
						}
						if ( bSelect && bCordova )
						{
							// note that on android, result.affectedRows is 1 even when inserted many rows
							result = result.rows || [];
						}

						//log.consoleLog("changes="+this.changes+", affected="+result.affectedRows);

						var consumed_time	= Date.now() - start_ts;
						if ( consumed_time > 25 )
						{
							log.consoleLog
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
				var start_ts = Date.now();
				if ( bCordova )
				{
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
	//	accumulate array of functions for async.series()
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
				//	add callback for async.series() member tasks
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
			log.consoleLog( "takeConnectionFromPool will wait for ready" );
			m_cEventEmitter.once
			(
				'ready',
				function()
				{
					log.consoleLog( "db is now ready" );
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
				//	log.consoleLog("reusing previously opened connection");
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
		//	log.consoleLog("queuing");
		//
		m_arrQueue.push( handleConnection );
	}


	function onDbReady()
	{
		if ( bCordova && ! cordovaSqlite )
		{
			cordovaSqlite = window.cordova.require( 'cordova-sqlite-plugin.SQLite' );
		}

		//	...
		m_bReady = true;
		m_cEventEmitter.emit('ready');
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
		//	log.consoleLog(arguments[0]);
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

function getDatabaseDirPath(){
	return getParentDirPath() + '/' + getDatabaseDirName();
}


function createDatabaseIfNecessary(db_name, onDbReady){
	
	log.consoleLog('createDatabaseIfNecessary '+db_name);
	var initial_db_filename = 'initial.' + db_name;

	// on mobile platforms, copy initial sqlite file from app root to data folder where we can open it for writing
	if (bCordova){
		log.consoleLog("will wait for deviceready");
		document.addEventListener("deviceready", function onDeviceReady(){
			log.consoleLog("deviceready handler");
			log.consoleLog("data dir: "+window.cordova.file.dataDirectory);
			log.consoleLog("app dir: "+window.cordova.file.applicationDirectory);
			window.requestFileSystem(LocalFileSystem.PERSISTENT, 0, function onFileSystemSuccess(fs){
				window.resolveLocalFileSystemURL(getDatabaseDirPath() + '/' + db_name, function(fileEntry){
					log.consoleLog("database file already exists");
					onDbReady();
				}, function onSqliteNotInited(err) { // file not found
					log.consoleLog("will copy initial database file");
					window.resolveLocalFileSystemURL(window.cordova.file.applicationDirectory + "/www/" + initial_db_filename, function(fileEntry) {
						log.consoleLog("got initial db fileentry");
						// get parent dir
						window.resolveLocalFileSystemURL(getParentDirPath(), function(parentDirEntry) {
							log.consoleLog("resolved parent dir");
							parentDirEntry.getDirectory(getDatabaseDirName(), {create: true}, function(dbDirEntry){
								log.consoleLog("resolved db dir");
								fileEntry.copyTo(dbDirEntry, db_name, function(){
									log.consoleLog("copied initial cordova database");
									onDbReady();
								}, function(err){
									throw Error("failed to copyTo: "+JSON.stringify(err));
								});
							}, function(err){
								throw Error("failed to getDirectory databases: "+JSON.stringify(err));
							});
						}, function(err){
							throw Error("failed to resolveLocalFileSystemURL of parent dir: "+JSON.stringify(err));
						});
					}, function(err){
						throw Error("failed to getFile: "+JSON.stringify(err));
					});
				});
			}, function onFailure(err){
				throw Error("failed to requestFileSystem: "+err);
			});
		}, false);
	}
	else{ // copy initial db to app folder
		var fs = require('fs'+'');
		fs.stat(path + db_name, function(err, stats){
			log.consoleLog("stat "+err);
			if (!err) // already exists
				return onDbReady();
			log.consoleLog("will copy initial db");
			var mode = parseInt('700', 8);
			var parent_dir = require('path'+'').dirname(path);
			fs.mkdir(parent_dir, mode, function(err){
				log.consoleLog('mkdir '+parent_dir+': '+err);
				fs.mkdir(path, mode, function(err){
					log.consoleLog('mkdir '+path+': '+err);
					fs.createReadStream(__dirname + '/' + initial_db_filename).pipe(fs.createWriteStream(path + db_name)).on('finish', onDbReady);
				});
			});
		});
	}
}




/**
 *	exports
 */
module.exports		= CSQLitePool;

