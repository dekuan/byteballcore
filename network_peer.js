/*jslint node: true */
"use strict";

var WebSocket			= process.browser ? global.WebSocket : require( 'ws' );
var socks			= process.browser ? null : require( 'socks' + '' );

var _async			= require( 'async' );
var _db				= require( './db.js' );
var _conf			= require( './conf.js' );
var _event_bus			= require( './event_bus.js' );
var _breadcrumbs		= require( './breadcrumbs.js' );

var _network_message		= require( './network_message.js' );



var m_arrInboundPeers				= [];		//	all clients connected in
var m_arrOutboundPeers				= [];		//	all peers server connected to

var m_oAssocConnectingOutboundWebSockets	= {};
var m_oAssocKnownPeers				= {};

var m_pfnOnWebSocketMessage			= null;
var m_pfnOnWebSocketClosed			= null;
var m_pfnSubscribe				= null;




//////////////////////////////////////////////////////////////////////
//	peers
//////////////////////////////////////////////////////////////////////



function setAddressOnWebSocketMessage( pfnAddress )
{
	m_pfnOnWebSocketMessage = pfnAddress;
}

function setAddressOnWebSocketClosed( pfnAddress )
{
	m_pfnOnWebSocketClosed = pfnAddress;
}

function setAddressSubscribe( pfnAddress )
{
	m_pfnSubscribe = pfnAddress;
}


function findNextPeer( ws, handleNextPeer )
{
	tryFindNextPeer
	(
		ws,
		function( next_ws )
		{
			var peer;

			if ( next_ws )
			{
				return handleNextPeer( next_ws );
			}

			//	...
			peer	= ws ? ws.peer : '[none]';
			console.log( 'findNextPeer after ' + peer + ' found no appropriate peer, will wait for a new connection' );

			//	...
			_event_bus.once
			(
				'connected_to_source',
				function( new_ws )
				{
					console.log( 'got new connection, retrying findNextPeer after ' + peer );
					findNextPeer( ws, handleNextPeer );
				}
			);
		}
	);
}


/**
 *	always pick the next peer as target to connect to
 *
 *	@param ws
 *	@param handleNextPeer
 */
function tryFindNextPeer( ws, handleNextPeer )
{
	var arrOutboundSources;
	var len;
	var peer_index;
	var next_peer_index;

	//
	//	bSource == true
	//	means:
	//		I connected to source after sending a 'subscribe' command to hub/server
	//
	arrOutboundSources	= m_arrOutboundPeers.filter( function( outbound_ws ) { return outbound_ws.bSource; } );
	len			= arrOutboundSources.length;

	if ( len > 0 )
	{
		//
		//	there are active outbound connections
		//

		//
		//	-1 if it is already disconnected by now,
		//		or if it is inbound peer,
		//		or if it is null
		//
		peer_index	= arrOutboundSources.indexOf( ws );
		next_peer_index	= ( peer_index === -1 ) ? getRandomInt( 0, len - 1 ) : ( ( peer_index + 1 ) % len );
		handleNextPeer( arrOutboundSources[ next_peer_index ] );
	}
	else
	{
		findRandomInboundPeer( handleNextPeer );
	}
}

function getRandomInt( min, max )
{
	return Math.floor( Math.random() * ( max + 1 - min ) ) + min;
}

function findRandomInboundPeer( handleInboundPeer )
{
	var arrInboundSources;
	var arrInboundHosts;

	//	...
	arrInboundSources	= m_arrInboundPeers.filter( function( inbound_ws ) { return inbound_ws.bSource; } );
	if ( arrInboundSources.length === 0 )
	{
		return handleInboundPeer( null );
	}

	//	...
	arrInboundHosts	= arrInboundSources.map( function( ws ) { return ws.host; } );

	//
	//	filter only those inbound peers that are reversible
	//
	_db.query
	(
		"SELECT peer_host \
		FROM peer_host_urls JOIN peer_hosts USING( peer_host ) \
		WHERE is_active = 1 AND peer_host IN( ? ) \
			AND ( \
				count_invalid_joints / count_new_good_joints < ? \
				OR \
				count_new_good_joints = 0 AND count_nonserial_joints = 0 AND count_invalid_joints = 0 \
			) \
		ORDER BY ( count_new_good_joints = 0 ), " + _db.getRandom() + " LIMIT 1",
		[
			arrInboundHosts,
			_conf.MAX_TOLERATED_INVALID_RATIO
		],
		function( rows )
		{
			var host;
			var ws;

			//	...
			console.log( rows.length + " inbound peers" );

			if ( rows.length === 0 )
			{
				return handleInboundPeer( null );
			}

			//	...
			host	= rows[ 0 ].peer_host;
			console.log( "selected inbound peer " + host );

			ws = arrInboundSources.filter
			(
				function( ws )
				{
					return ( ws.host === host );
				}
			)[ 0 ];

			if ( ! ws )
			{
				throw Error( "inbound ws not found" );
			}

			//	...
			handleInboundPeer( ws );
		}
	);
}


function connectToPeer( url, onOpen )
{
	var options;
	var ws;

	//	...
	addPeer( url );

	//	...
	options	= {};
	if ( socks && _conf.socksHost && _conf.socksPort )
	{
		options.agent	= new socks.Agent
		(
			{
				proxy		:
				{
					ipaddress	: _conf.socksHost,
					port		: _conf.socksPort,
					type		: 5
				}
			},
			/^wss/i.test( url )
		);
		console.log( 'Using proxy: ' + _conf.socksHost + ':' + _conf.socksPort );
	}

	//	...
	ws = options.agent ? new WebSocket( url, options ) : new WebSocket( url );
	m_oAssocConnectingOutboundWebSockets[ url ] = ws;

	setTimeout
	(
		function()
		{
			if ( m_oAssocConnectingOutboundWebSockets[ url ] )
			{
				console.log( 'abandoning connection to ' + url + ' due to timeout' );
				delete m_oAssocConnectingOutboundWebSockets[ url ];
				m_oAssocConnectingOutboundWebSockets[ url ]	= null;

				//
				//	after this,
				//	new connection attempts will be allowed to the wire,
				// 	but this one can still succeed.
				//
				//	See the check for duplicates below.
				//
			}
		},
		5000
	);

	//	avoid warning
	ws.setMaxListeners( 20 );
	ws.once
	(
		'open',
		function onWsOpen()
		{
			var another_ws_to_same_peer;

			//	...
			_breadcrumbs.add( 'connected to ' + url );
			delete m_oAssocConnectingOutboundWebSockets[ url ];
			m_oAssocConnectingOutboundWebSockets[ url ]	= null;

			//	...
			ws.assocPendingRequests		= {};
			ws.assocInPreparingResponse	= {};

			if ( ! ws.url )
			{
				throw Error( "no url on ws" );
			}

			//	browser implementatin of Websocket might add "/"
			if ( ws.url !== url && ws.url !== url + "/" )
			{
				throw Error( "url is different: " + ws.url );
			}

			//	...
			another_ws_to_same_peer	= getOutboundPeerWsByUrl( url );
			if ( another_ws_to_same_peer )
			{
				//
				//	duplicate connection.
				//	May happen if we abondoned a connection attempt after timeout but it still succeeded while we opened another connection
				//
				console.log( 'already have a connection to ' + url + ', will keep the old one and close the duplicate' );
				ws.close( 1000, 'duplicate connection' );

				if ( onOpen )
				{
					onOpen( null, another_ws_to_same_peer );
				}

				return;
			}

			//	...
			ws.peer		= url;
			ws.host		= getHostByPeer( ws.peer );
			ws.bOutbound	= true;
			ws.last_ts	= Date.now();

			console.log( 'connected to ' + url + ", host " + ws.host );
			m_arrOutboundPeers.push( ws );
			_network_message.sendVersion( ws );

			//	I can listen too, this is my url to connect to
			if ( _conf.myUrl )
			{
				_network_message.sendJustSaying( ws, 'my_url', _conf.myUrl );
			}

			if ( ! _conf.bLight )
			{
				if ( 'function' === typeof m_pfnSubscribe )
				{
					m_pfnSubscribe.call( this, ws );
				}
			}

			if ( onOpen )
			{
				onOpen( null, ws );
			}

			//	...
			_event_bus.emit( 'connected', ws );
			_event_bus.emit( 'open-' + url );
		}
	);

	ws.on
	(
		'close',
		function onWsClose()
		{
			var i;

			//	...
			i	= m_arrOutboundPeers.indexOf( ws );
			console.log( 'close event, removing ' + i + ': ' + url );

			if ( i !== -1 )
			{
				m_arrOutboundPeers.splice( i, 1 );
			}

			//	...
			if ( 'function' === typeof m_pfnOnWebSocketClosed )
			{
				m_pfnOnWebSocketClosed.call( this, ws );
			}

			if ( options.agent &&
				options.agent.destroy )
			{
				options.agent.destroy();
			}
		}
	);

	ws.on
	(
		'error',
		function onWsError( e )
		{
			var err;

			//	...
			delete m_oAssocConnectingOutboundWebSockets[ url ];
			m_oAssocConnectingOutboundWebSockets[ url ]	= null;
			console.log( "error from server " + url + ": " + e );

			//	...
			err	= e.toString();
			//	! ws.bOutbound means not connected yet. This is to distinguish connection errors from later errors that occur on open connection

			if ( ! ws.bOutbound && onOpen )
			{
				onOpen( err );
			}

			if ( ! ws.bOutbound )
			{
				_event_bus.emit( 'open-' + url, err );
			}
		}
	);

	//
	//	...
	//
	if ( 'function' === typeof m_pfnOnWebSocketMessage )
	{
		ws.on
		(
			'message',
			m_pfnOnWebSocketMessage
		);
	}

	//	...
	console.log( 'connectToPeer done' );
}


/**
 *	try to add outbound peers
 */
function addOutboundPeers( multiplier )
{
	var order_by;
	var arrOutboundPeerUrls;
	var arrInboundHosts;
	var max_new_outbound_peers;

	if ( ! multiplier )
	{
		multiplier = 1;
	}
	if ( multiplier >= 32 )
	{
		//	limit recursion
		return;
	}

	//
	//	don't stick to old peers with most accumulated good joints
	//
	order_by		= ( multiplier <= 4 ) ? "count_new_good_joints DESC" : _db.getRandom();
	arrOutboundPeerUrls	= m_arrOutboundPeers.map
	(
		function( ws )
		{
			return ws.peer;
		}
	);
	arrInboundHosts		= m_arrInboundPeers.map
	(
		function( ws )
		{
			return ws.host;
		}
	);

	//	having too many connections being opened creates odd delays in _db functions
	max_new_outbound_peers	= Math.min( _conf.MAX_OUTBOUND_CONNECTIONS - arrOutboundPeerUrls.length, 5 );
	if ( max_new_outbound_peers <= 0 )
	{
		return;
	}

	//
	//	TODO
	//	LONG SQL, BUT FAST, CAUSE FEW DATA
	//
	//	Questions:
	//	1, What's the different among [peers], [peer_hosts], [peer_host_urls] ?
	//	2, INVALID_RATIO = count_invalid_joints / count_new_good_joints, if 0/0 ?
	//
	_db.query
	(
		"SELECT peer \
		FROM peers \
		JOIN peer_hosts USING(peer_host) \
		LEFT JOIN peer_host_urls ON peer=url AND is_active=1 \
		WHERE ( \
			count_invalid_joints / count_new_good_joints < ? \
			OR count_new_good_joints = 0 AND count_nonserial_joints = 0 AND count_invalid_joints = 0 \
		      ) \
			" + ( ( arrOutboundPeerUrls.length > 0 ) ? " AND peer NOT IN(" + _db.escape( arrOutboundPeerUrls ) + ") " : "" ) + " \
			" + ( ( arrInboundHosts.length > 0 ) ? " AND (peer_host_urls.peer_host IS NULL OR peer_host_urls.peer_host NOT IN(" + _db.escape( arrInboundHosts ) + ")) " : "" ) + " \
			AND is_self=0 \
		ORDER BY " + order_by + " LIMIT ?",
		[
			_conf.MAX_TOLERATED_INVALID_RATIO * multiplier,
			max_new_outbound_peers
		],
		function( rows )
		{
			var i;

			//
			//	TODO
			//	find outbound peer or connect ?
			//
			for ( i = 0; i < rows.length; i ++ )
			{
				m_oAssocKnownPeers[ rows[ i ].peer ] = true;
				findOutboundPeerOrConnect( rows[ i ].peer );
			}

			//	if no outbound connections at all, get less strict
			if ( arrOutboundPeerUrls.length === 0 && rows.length === 0 )
			{
				addOutboundPeers( multiplier * 2 );
			}
		}
	);
}

function getHostByPeer( peer )
{
	var matches;

	//	...
	matches	= peer.match( /^wss?:\/\/(.*)$/i );
	if ( matches )
	{
		peer = matches[ 1 ];
	}

	matches	= peer.match( /^(.*?)[:\/]/ );
	return matches ? matches[ 1 ] : peer;
}

function addPeerHost( host, onDone )
{
	_db.query
	(
		"INSERT " + _db.getIgnore() + " INTO peer_hosts ( peer_host ) VALUES ( ? )",
		[
			host
		],
		function()
		{
			if ( onDone )
			{
				onDone();
			}
		}
	);
}

function addPeer( peer )
{
	var host;

	if ( m_oAssocKnownPeers[ peer ] )
	{
		return;
	}

	//	...
	m_oAssocKnownPeers[ peer ] = true;
	host = getHostByPeer( peer );

	//	...
	addPeerHost
	(
		host,
		function()
		{
			console.log( "will insert peer " + peer );
			_db.query
			(
				"INSERT " + _db.getIgnore() + " INTO peers ( peer_host, peer ) VALUES ( ?, ? )",
				[
					host,
					peer
				]
			);
		}
	);
}

function getOutboundPeerWsByUrl( url )
{
	var i;

	//	...
	console.log( "outbound peers: " + m_arrOutboundPeers.map( function( o ){ return o.peer; } ).join( ", " ) );

	for ( i = 0; i < m_arrOutboundPeers.length; i ++ )
	{
		if ( m_arrOutboundPeers[ i ].peer === url )
		{
			//	...
			return m_arrOutboundPeers[ i ];
		}
	}

	return null;
}

function getPeerWebSocket( peer )
{
	var i;

	for ( i = 0; i < m_arrOutboundPeers.length; i ++ )
	{
		if ( m_arrOutboundPeers[ i ].peer === peer )
		{
			//	...
			return m_arrOutboundPeers[ i ];
		}
	}

	for ( i = 0; i < m_arrInboundPeers.length; i ++ )
	{
		if ( m_arrInboundPeers[ i ].peer === peer )
		{
			//	...
			return m_arrInboundPeers[ i ];
		}
	}

	return null;
}

function findOutboundPeerOrConnect( url, onOpen )
{
	var ws;

	if ( ! url )
	{
		throw Error( 'no url' );
	}
	if ( ! onOpen )
	{
		onOpen = function(){};
	}

	//	...
	url	= url.toLowerCase();
	ws	= getOutboundPeerWsByUrl( url );
	if ( ws )
	{
		return onOpen( null, ws );
	}

	//	check if we are already connecting to the peer
	ws = m_oAssocConnectingOutboundWebSockets[ url ];
	if ( ws )
	{
		//	add second event handler
		_breadcrumbs.add( 'already connecting to ' + url );
		return _event_bus.once
		(
			'open-' + url,
			function secondOnOpen( err )
			{
				console.log('second open '+url+", err="+err);

				if ( err )
				{
					return onOpen( err );
				}

				if ( ws.readyState === ws.OPEN )
				{
					onOpen( null, ws );
				}
				else
				{
					//
					//	can happen
					//	e.g. if the ws was abandoned but later succeeded, we opened another connection in the meantime,
					//	and had another_ws_to_same_peer on the first connection
					//
					console.log( 'in second onOpen, websocket already closed' );
					onOpen( '[internal] websocket already closed' );
				}
			}
		);
	}

	console.log( "will connect to " + url );

	//
	//	...
	//
	connectToPeer( url, onOpen );
}

function purgePeerEvents()
{
	if ( _conf.storage !== 'sqlite' )
	{
		return;
	}

	console.log( 'will purge peer events' );
	_db.query
	(
		"DELETE FROM peer_events WHERE event_date <= datetime('now', '-3 day')",
		function()
		{
        		console.log("deleted some old peer_events");
		}
	);
}

/**
 *	try to purge dead peers
 */
function purgeDeadPeers()
{
	var arrOutboundPeerUrls;

	if ( _conf.storage !== 'sqlite' )
	{
		//	for SQLite only
		return;
	}

	//	...
	console.log( 'will purge dead peers' );
	arrOutboundPeerUrls = m_arrOutboundPeers.map
	(
		function( ws )
		{
			return ws.peer;
		}
	);

	//
	//	rowid is a 64-bit signed integer
	//	The rowid column is a key that uniquely identifies the row within its table.
	//	The table that has rowid column is called rowid table.
	//
	_db.query
	(
		"SELECT rowid, " + _db.getUnixTimestamp( 'event_date' ) + " AS ts " +
		"FROM peer_events " +
		"ORDER BY rowid DESC " +
		"LIMIT 1",
		function( lrows )
		{
			var last_rowid;
			var last_event_ts;

			if ( lrows.length === 0 )
			{
				return;
			}

			//	the last rowid and event ts
			last_rowid	= lrows[ 0 ].rowid;
			last_event_ts	= lrows[ 0 ].ts;

			//	...
			_db.query
			(
				"SELECT peer, peer_host FROM peers",
				function( rows )
				{
					//	...
					_async.eachSeries
					(
						rows,
						function( row, cb )
						{
							if ( arrOutboundPeerUrls.indexOf( row.peer ) >= 0 )
							{
								return cb();
							}

							_db.query
							(
								"SELECT MAX(rowid) AS max_rowid, " +
								"MAX(" + _db.getUnixTimestamp( 'event_date' ) + ") AS max_event_ts " +
								"FROM peer_events WHERE peer_host=?",
								[
									row.peer_host
								],
								function( mrows )
								{
									var max_rowid;
									var max_event_ts;
									var count_other_events;
									var days_since_last_event;

									//	...
									max_rowid		= mrows[ 0 ].max_rowid || 0;
									max_event_ts		= mrows[ 0 ].max_event_ts || 0;
									count_other_events	= last_rowid - max_rowid;
									days_since_last_event	= ( last_event_ts - max_event_ts ) / 24 / 3600;

									if ( count_other_events < 20000 || days_since_last_event < 7 )
									{
										return cb();
									}

									//	...
									console.log( 'peer ' + row.peer + ' is dead, will delete' );
									_db.query
									(
										"DELETE FROM peers WHERE peer=?",
										[
											row.peer
										],
										function()
										{
											delete m_oAssocKnownPeers[ row.peer ];
											m_oAssocKnownPeers[ row.peer ] = null;
											cb();
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


function getOutboundPeers()
{
	return m_arrOutboundPeers;
}

function getAssocConnectingOutboundWebSockets()
{
	return m_oAssocConnectingOutboundWebSockets;
}







/**
 *	exports
 */
exports.setAddressOnWebSocketMessage			= setAddressOnWebSocketMessage;
exports.setAddressOnWebSocketClosed			= setAddressOnWebSocketClosed;
exports.setAddressSubscribe				= setAddressSubscribe;

exports.findNextPeer					= findNextPeer;
exports.tryFindNextPeer					= tryFindNextPeer;
exports.getRandomInt					= getRandomInt;
exports.findRandomInboundPeer				= findRandomInboundPeer;
exports.connectToPeer					= connectToPeer;
exports.addOutboundPeers				= addOutboundPeers;
exports.getHostByPeer					= getHostByPeer;
exports.addPeerHost					= addPeerHost;
exports.addPeer						= addPeer;
exports.getOutboundPeerWsByUrl				= getOutboundPeerWsByUrl;
exports.getPeerWebSocket				= getPeerWebSocket;
exports.findOutboundPeerOrConnect			= findOutboundPeerOrConnect;
exports.purgePeerEvents					= purgePeerEvents;
exports.purgeDeadPeers					= purgeDeadPeers;

exports.getOutboundPeers				= getOutboundPeers;
exports.getAssocConnectingOutboundWebSockets		= getAssocConnectingOutboundWebSockets;

