/*jslint node: true */
"use strict";

var WebSocket			= process.browser ? global.WebSocket : require( 'ws' );
var socks			= process.browser ? null : require('socks'+'');
var WebSocketServer		= WebSocket.Server;

var _				= require( 'lodash' );
var _crypto			= require( 'crypto' );
var _async			= require( 'async' );
var _log			= require( './log.js' );
var _logex			= require( './logex.js' );
var _db				= require( './db.js' );
var _constants			= require( './constants.js' );
var _storage			= require( './storage.js' );
var _my_witnesses		= require( './my_witnesses.js' );
var _joint_storage		= require( './joint_storage.js' );
var _validation			= require( './validation.js' );
var _validation_utils		= require( './validation_utils.js' );
var _writer			= require( './writer.js' );
var _conf			= require( './conf.js' );
var _mutex			= require( './mutex.js' );
var _catchup			= require( './catchup.js' );
var _private_payment		= require( './private_payment.js' );
var _object_hash		= require( './object_hash.js' );
var _ecdsa_sig			= require( './signature.js' );
var _event_bus			= require( './event_bus.js' );
var _light			= require( './light.js' );
var _breadcrumbs		= require( './breadcrumbs.js' );
var _profilerex			= require( './profilerex.js' );

var _mail			= process.browser ? null : require( './mail.js' + '' );

var _network_consts		= require( './network_consts.js' );



var m_oWss;
var m_arrOutboundPeers				= [];
var m_oAssocConnectingOutboundWebSockets	= {};
var m_oAssocUnitsInWork				= {};
var m_oAssocRequestedUnits			= {};
var m_bCatchingUp				= false;
var m_bWaitingForCatchupChain			= false;
var m_bWaitingTillIdle				= false;
var m_nComingOnlineTime				= Date.now();
var m_oAssocReroutedConnectionsByTag		= {};
var m_arrWatchedAddresses			= [];		//	does not include my addresses, therefore always empty
var m_nLastHearbeatWakeTs			= Date.now();
var m_arrPeerEventsBuffer			= [];
var m_oAssocKnownPeers				= {};
var m_exchangeRates				= {};



/**
 *	if not using a hub and accepting messages directly (be your own hub)
 */
var m_sMyDeviceAddress;
var m_objMyTempPubkeyPackage;


exports.light_vendor_url = null;







function setMyDeviceProps( device_address, objTempPubkey )
{
	m_sMyDeviceAddress	= device_address;
	m_objMyTempPubkeyPackage	= objTempPubkey;
}








//////////////////////////////////////////////////////////////////////
//	general network functions
//////////////////////////////////////////////////////////////////////


function sendMessage( ws, type, content )
{
	var message;

	//	...
	message	= JSON.stringify( [ type, content ] );

	if ( ws.readyState !== ws.OPEN )
	{
		return _log.consoleLog( "readyState=" + ws.readyState + ' on peer ' + ws.peer + ', will not send ' + message );
	}

	_log.consoleLog( "SENDING " + message + " to " + ws.peer );
	ws.send( message );
}

function sendJustSaying( ws, subject, body )
{
	sendMessage( ws, 'justsaying', { subject : subject, body : body } );
}

function sendAllInboundJustSaying( subject, body )
{
	m_oWss.arrClients.forEach
	(
		function( ws )
		{
			sendMessage( ws, 'justsaying', { subject: subject, body: body } );
		}
	);
}

function sendError( ws, error )
{
	sendJustSaying( ws, 'error', error );
}

function sendInfo( ws, content )
{
	sendJustSaying( ws, 'info', content );
}

function sendResult( ws, content )
{
	sendJustSaying( ws, 'result', content );
}

function sendErrorResult( ws, unit, error )
{
	sendResult( ws, { unit : unit, result : 'error', error : error } );
}

function sendVersion( ws )
{
	var libraryPackageJson;

	//	...
	libraryPackageJson	= require( './package.json' );

	//	...
	sendJustSaying
	(
		ws,
		'version',
		{
			protocol_version	: _constants.version,
			alt			: _constants.alt,
			library			: libraryPackageJson.name,
			library_version		: libraryPackageJson.version,
			program			: _conf.program,
			program_version		: _conf.program_version
		}
	);
}

function sendResponse( ws, tag, response )
{
	delete ws.assocInPreparingResponse[ tag ];
	sendMessage( ws, 'response', { tag: tag, response: response } );
}

function sendErrorResponse( ws, tag, error )
{
	sendResponse( ws, tag, { error : error } );
}


/**
 *	if a 2nd identical request is issued before we receive a response to the 1st request, then:
 *	1. its pfnResponseHandler will be called too but no second request will be sent to the wire
 *	2. bReRoutable flag must be the same
 *
 *	@param ws
 *	@param command
 *	@param params
 *	@param bReRoutable
 *	@param pfnResponseHandler
 */
function sendRequest( ws, command, params, bReRoutable, pfnResponseHandler )
{
	var request;
	var content;
	var tag;
	var pfnReroute;
	var nRerouteTimer;
	var nCancelTimer;

	//	...
	request = { command : command };

	if ( params )
	{
		request.params = params;
	}

	//
	//	tag like : w35dxwqyQ2CzqHkOG5q+gwagPtaPweD4LEwzC2RjQNo=
	//
	content	= _.clone( request );
	tag	= _object_hash.getBase64Hash( request );

	//
	//	if (ws.assocPendingRequests[tag]) // ignore duplicate requests while still waiting for response from the same peer
	//	return _log.consoleLog("will not send identical "+command+" request");
	//
	if ( ws.assocPendingRequests[ tag ] )
	{
		_log.consoleLog
		(
			'already sent a ' + command + ' request to ' + ws.peer + ', will add one more response handler rather than sending a duplicate request to the wire'
		);
		ws.assocPendingRequests[ tag ].responseHandlers.push( pfnResponseHandler );
		return;
	}


	//
	//	...
	//
	content.tag	= tag;

	//
	//	after _network_consts.STALLED_TIMEOUT, reroute the request to another peer
	//	it'll work correctly even if the current peer is already disconnected when the timeout fires
	//
	//	THIS function will be called when the request is timeout
	//
	pfnReroute = bReRoutable ? function()
	{
		_log.consoleLog( 'will try to reroute a ' + command + ' request stalled at ' + ws.peer );

		if ( ! ws.assocPendingRequests[ tag ] )
		{
			return _log.consoleLog( 'will not reroute - the request was already handled by another peer' );
		}

		//	...
		ws.assocPendingRequests[ tag ].bRerouted	= true;
		findNextPeer
		(
			ws,
			function( next_ws )
			{
				//
				//	the callback may be called much later if findNextPeer has to wait for connection
				//
				if ( ! ws.assocPendingRequests[ tag ] )
				{
					return _log.consoleLog( 'will not reroute after findNextPeer - the request was already handled by another peer' );
				}

				if ( next_ws === ws ||
					m_oAssocReroutedConnectionsByTag[ tag ] && m_oAssocReroutedConnectionsByTag[ tag ].indexOf( next_ws ) >= 0 )
				{
					_log.consoleLog( 'will not reroute ' + command + ' to the same peer, will rather wait for a new connection' );
					_event_bus.once
					(
						'connected_to_source',
						function()
						{
							//	try again
							_log.consoleLog( 'got new connection, retrying reroute ' + command );
							pfnReroute();
						}
					);
					return;
				}

				//
				//	SEND REQUEST AGAIN FOR EVERY responseHandlers
				//
				_log.consoleLog( 'rerouting ' + command + ' from ' + ws.peer + ' to ' + next_ws.peer );
				ws.assocPendingRequests[ tag ].responseHandlers.forEach
				(
					function( rh )
					{
						sendRequest( next_ws, command, params, bReRoutable, rh );
					}
				);

				//
				//	push to cache
				//
				if ( ! m_oAssocReroutedConnectionsByTag[ tag ] )
				{
					m_oAssocReroutedConnectionsByTag[ tag ] = [ ws ];
				}
				m_oAssocReroutedConnectionsByTag[ tag ].push( next_ws );
			}
		);
	}
	: null;

	//
	//	for request
	//
	nRerouteTimer	= bReRoutable
		? setTimeout
		(
			//	callback handler while the request is TIMEOUT
			function ()
			{
				_log.consoleLog( '# network::sendRequest request ' + command + ', send to ' + ws.peer + ' was overtime.' );
				pfnReroute.apply( this, arguments );
			},
			_network_consts.STALLED_TIMEOUT
		)
		: null;

	//
	//	for response
	//
	nCancelTimer	= bReRoutable
		? null
		: setTimeout
		(
			function()
			{
				_log.consoleLog( '# network::sendRequest request ' + command + ', response from ' + ws.peer + ' was overtime.' );

				//
				//	delete all overtime requests/connections in pending requests list
				//
				ws.assocPendingRequests[ tag ].responseHandlers.forEach
				(
					function( rh )
					{
						rh( ws, request, { error: "[internal] response timeout" } );
					}
				);
				delete ws.assocPendingRequests[ tag ];
			},
			_network_consts.RESPONSE_TIMEOUT
		);

	//
	//	build pending request list
	//
	ws.assocPendingRequests[ tag ] =
	{
		request			: request,
		responseHandlers	: [ pfnResponseHandler ],
		reroute			: pfnReroute,
		reroute_timer		: nRerouteTimer,
		cancel_timer		: nCancelTimer
	};

	//
	//	...
	//
	sendMessage( ws, 'request', content );
}


function handleResponse( ws, tag, response )
{
	var pendingRequest;

	//	...
	pendingRequest	= ws.assocPendingRequests[ tag ];

	//	was canceled due to timeout or rerouted and answered by another peer
	if ( ! pendingRequest )
	{
		//	throw "no req by tag "+tag;
		return _log.consoleLog( "no req by tag " + tag );
	}

	//	...
	pendingRequest.responseHandlers.forEach
	(
		function( pfnResponseHandler )
		{
			process.nextTick( function()
			{
				pfnResponseHandler( ws, pendingRequest.request, response );
			});
		}
	);

	clearTimeout( pendingRequest.reroute_timer );
	clearTimeout( pendingRequest.cancel_timer );
	delete ws.assocPendingRequests[ tag ];

	//
	//	if the request was rerouted, cancel all other pending requests
	//
	if ( m_oAssocReroutedConnectionsByTag[ tag ] )
	{
		m_oAssocReroutedConnectionsByTag[ tag ].forEach
		(
			function( client )
			{
				if ( client.assocPendingRequests[ tag ] )
				{
					clearTimeout( client.assocPendingRequests[ tag ].reroute_timer );
					clearTimeout( client.assocPendingRequests[ tag ].cancel_timer );
					delete client.assocPendingRequests[ tag ];
				}
			}
		);
		delete m_oAssocReroutedConnectionsByTag[ tag ];
	}
}

function cancelRequestsOnClosedConnection( ws )
{
	var tag;
	var pendingRequest;

	_log.consoleLog( "websocket closed, will complete all outstanding requests" );

	for ( tag in ws.assocPendingRequests )
	{
		//	...
		pendingRequest	= ws.assocPendingRequests[ tag ];

		//	...
		clearTimeout( pendingRequest.reroute_timer );
		clearTimeout( pendingRequest.cancel_timer );

		//	reroute immediately, not waiting for _network_consts.STALLED_TIMEOUT
		if ( pendingRequest.reroute )
		{
			if ( ! pendingRequest.bRerouted )
			{
				pendingRequest.reroute();
			}
			//	we still keep ws.assocPendingRequests[tag] because we'll need it when we find a peer to reroute to
		}
		else
		{
			pendingRequest.responseHandlers.forEach( function( rh )
			{
				rh( ws, pendingRequest.request, { error : "[internal] connection closed" } );
			});

			delete ws.assocPendingRequests[ tag ];
			ws.assocPendingRequests[ tag ]	= null;
		}
	}

	//	...
	printConnectionStatus();
}







//////////////////////////////////////////////////////////////////////
//	peers
//////////////////////////////////////////////////////////////////////

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
			_log.consoleLog( 'findNextPeer after ' + peer + ' found no appropriate peer, will wait for a new connection' );

			//	...
			_event_bus.once
			(
				'connected_to_source',
				function( new_ws )
				{
					_log.consoleLog( 'got new connection, retrying findNextPeer after ' + peer );
					findNextPeer( ws, handleNextPeer );
				}
			);
		}
	);
}

function tryFindNextPeer( ws, handleNextPeer )
{
	var arrOutboundSources;
	var len;
	var peer_index;
	var next_peer_index;

	//	...
	arrOutboundSources = m_arrOutboundPeers.filter
	(
		function( outbound_ws )
		{
			return outbound_ws.bSource;
		}
	);
	len = arrOutboundSources.length;

	//	...
	if ( len > 0 )
	{
		//	-1 if it is already disconnected by now, or if it is inbound peer, or if it is null
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
	arrInboundSources	= m_oWss.arrClients.filter
	(
		function( inbound_ws )
		{
			return inbound_ws.bSource;
		}
	);

	if ( arrInboundSources.length === 0 )
	{
		return handleInboundPeer(null);
	}

	//	...
	arrInboundHosts	= arrInboundSources.map
	(
		function( ws )
		{
			return ws.host;
		}
	);

	//	filter only those inbound peers that are reversible
	_db.query
	(
		"SELECT peer_host FROM peer_host_urls JOIN peer_hosts USING( peer_host ) \n\
			WHERE is_active = 1 AND peer_host IN( ? ) \n\
			AND ( count_invalid_joints / count_new_good_joints < ? \n\
			OR count_new_good_joints = 0 AND count_nonserial_joints = 0 AND count_invalid_joints = 0 ) \n\
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
			_log.consoleLog( rows.length + " inbound peers" );

			if ( rows.length === 0 )
			{
				return handleInboundPeer(null);
			}

			//	...
			host	= rows[ 0 ].peer_host;
			_log.consoleLog( "selected inbound peer " + host );

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

/**
 *	check and add peers
 */
function checkIfHaveEnoughOutboundPeersAndAdd()
{
	var arrOutboundPeerUrls;

	//	...
	arrOutboundPeerUrls = m_arrOutboundPeers.map
	(
		function( ws )
		{
			return ws.peer;
		}
	);

	//
	//	select peers good_joints > 0 and ...
	//
	_db.query
	(
		"SELECT peer FROM peers JOIN peer_hosts USING( peer_host ) \n\
			WHERE count_new_good_joints > 0 \n\
			AND count_invalid_joints / count_new_good_joints < ? \n\
			AND peer IN( ? )",
		[
			_conf.MAX_TOLERATED_INVALID_RATIO,
			( arrOutboundPeerUrls.length > 0 ) ? arrOutboundPeerUrls : null
		],
		function( rows )
		{
			var count_good_peers;
			var arrGoodPeerUrls;
			var i;
			var ws;

			//	...
			count_good_peers = rows.length;
			if ( count_good_peers >= _conf.MIN_COUNT_GOOD_PEERS )
			{
				//	larger then limitation
				return;
			}
			if ( count_good_peers === 0 )
			{
				//	nobody trusted enough to ask for new peers, can't do anything
				return;
			}

			//
			//	good peers
			//
			arrGoodPeerUrls	= rows.map
			(
				function( row )
				{
					return row.peer;
				}
			);

			for ( i = 0; i < m_arrOutboundPeers.length; i++ )
			{
				ws = m_arrOutboundPeers[ i ];
				if ( arrGoodPeerUrls.indexOf( ws.peer ) !== -1 )
				{
					//
					//	peer was not found in m_arrOutboundPeers
					//
					//	* try to send request to get peers
					//
					_log.consoleLog( "****** peer was not found in m_arrOutboundPeers, * try to send request to get peers" );
					requestPeers( ws );
				}
			}
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
		_log.consoleLog( 'Using proxy: ' + _conf.socksHost + ':' + _conf.socksPort );
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
				_log.consoleLog( 'abandoning connection to ' + url + ' due to timeout' );
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
				_log.consoleLog( 'already have a connection to ' + url + ', will keep the old one and close the duplicate' );
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

			_log.consoleLog( 'connected to ' + url + ", host " + ws.host );
			m_arrOutboundPeers.push( ws );
			sendVersion( ws );

			//	I can listen too, this is my url to connect to
			if ( _conf.myUrl )
			{
				sendJustSaying( ws, 'my_url', _conf.myUrl );
			}

			if ( ! _conf.bLight )
			{
				subscribe( ws );
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
			_log.consoleLog( 'close event, removing ' + i + ': ' + url );

			if ( i !== -1 )
			{
				m_arrOutboundPeers.splice( i, 1 );
			}

			//	...
			cancelRequestsOnClosedConnection( ws );

			if ( options.agent && options.agent.destroy )
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
			_log.consoleLog( "error from server " + url + ": " + e );

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

	ws.on
	(
		'message',
		onWebSocketMessage
	);

	//	...
	_log.consoleLog( 'connectToPeer done' );
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
	arrInboundHosts		= m_oWss.arrClients.map
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
		"SELECT peer \n\
		FROM peers \n\
		JOIN peer_hosts USING(peer_host) \n\
		LEFT JOIN peer_host_urls ON peer=url AND is_active=1 \n\
		WHERE ( \n\
			count_invalid_joints / count_new_good_joints < ? \n\
			OR count_new_good_joints = 0 AND count_nonserial_joints = 0 AND count_invalid_joints = 0 \n\
		      ) \n\
			" + ( ( arrOutboundPeerUrls.length > 0 ) ? " AND peer NOT IN(" + _db.escape( arrOutboundPeerUrls ) + ") \n" : "" ) + "\n\
			" + ( ( arrInboundHosts.length > 0 ) ? " AND (peer_host_urls.peer_host IS NULL OR peer_host_urls.peer_host NOT IN(" + _db.escape( arrInboundHosts ) + ")) \n" : "" ) + "\n\
			AND is_self=0 \n\
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
			_log.consoleLog( "will insert peer " + peer );
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
	_log.consoleLog( "outbound peers: " + m_arrOutboundPeers.map( function( o ){ return o.peer; } ).join( ", " ) );

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

	for ( i = 0; i < m_oWss.arrClients.length; i ++ )
	{
		if ( m_oWss.arrClients[ i ].peer === peer )
		{
			//	...
			return m_oWss.arrClients[ i ];
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
				_log.consoleLog('second open '+url+", err="+err);

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
					_log.consoleLog( 'in second onOpen, websocket already closed' );
					onOpen( '[internal] websocket already closed' );
				}
			}
		);
	}

	_log.consoleLog( "will connect to " + url );

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

	_log.consoleLog( 'will purge peer events' );
	_db.query
	(
		"DELETE FROM peer_events WHERE event_date <= datetime('now', '-3 day')",
		function()
		{
        		_log.consoleLog("deleted some old peer_events");
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
	_log.consoleLog( 'will purge dead peers' );
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
									_log.consoleLog( 'peer ' + row.peer + ' is dead, will delete' );
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


/**
 *	send request for getting peers
 */
function requestPeers( ws )
{
	sendRequest( ws, 'get_peers', null, false, handleNewPeers );
}

function handleNewPeers( ws, request, arrPeerUrls )
{
	var arrQueries;
	var i;
	var url;
	var regexp;
	var host;

	if ( arrPeerUrls.error )
	{
		return _log.consoleLog( 'get_peers failed: ' + arrPeerUrls.error );
	}
	if ( ! Array.isArray( arrPeerUrls ) )
	{
		return sendError( ws, "peer urls is not an array" );
	}

	//	...
	arrQueries = [];
	for ( i = 0; i < arrPeerUrls.length; i++ )
	{
		url	= arrPeerUrls[ i ];

		if ( _conf.myUrl && _conf.myUrl.toLowerCase() === url.toLowerCase() )
		{
			continue;
		}

		//	...
		regexp	= ( _conf.WS_PROTOCOL === 'wss://' ) ? /^wss:\/\// : /^wss?:\/\//;
		if ( ! url.match( regexp ) )
		{
			_log.consoleLog( 'ignoring new peer ' + url + ' because of incompatible ws protocol' );
			continue;
		}

		host	= getHostByPeer( url );
		_db.addQuery
		(
			arrQueries,
			"INSERT " + _db.getIgnore() + " INTO peer_hosts (peer_host) VALUES (?)",
			[ host ]
		);
		_db.addQuery
		(
			arrQueries,
			"INSERT " + _db.getIgnore() + " INTO peers (peer_host, peer, learnt_from_peer_host) VALUES(?,?,?)",
			[ host, url, ws.host ]
		);
	}

	//	...
	_async.series( arrQueries );
}






/**
 *	*
 *	heartbeat
 *	about every 3 seconds
 */
function heartbeat()
{
	var bJustResumed;

	//	just resumed after sleeping
	bJustResumed		= ( typeof window !== 'undefined' && window && window.cordova && Date.now() - m_nLastHearbeatWakeTs > 2 * _network_consts.HEARTBEAT_TIMEOUT );
	m_nLastHearbeatWakeTs	= Date.now();

	//
	//	The concat() method is used to merge two or more arrays.
	//	This method does not change the existing arrays, but instead returns a new array.
	//
	m_oWss.arrClients.concat( m_arrOutboundPeers ).forEach( function( ws )
	{
		var elapsed_since_last_received;
		var elapsed_since_last_sent_heartbeat;

		if ( ws.bSleeping || ws.readyState !== ws.OPEN )
		{
			//	web socket is not ready
			return;
		}

		//	...
		elapsed_since_last_received	= Date.now() - ws.last_ts;
		if ( elapsed_since_last_received < _network_consts.HEARTBEAT_TIMEOUT )
		{
			return;
		}

		if ( ! ws.last_sent_heartbeat_ts || bJustResumed )
		{
			ws.last_sent_heartbeat_ts	= Date.now();
			return sendRequest( ws, 'heartbeat', null, false, handleHeartbeatResponse );
		}

		//	...
		elapsed_since_last_sent_heartbeat	= Date.now() - ws.last_sent_heartbeat_ts;
		if ( elapsed_since_last_sent_heartbeat < _network_consts.HEARTBEAT_RESPONSE_TIMEOUT )
		{
			return;
		}

		//	...
		_log.consoleLog( 'will disconnect peer ' + ws.peer + ' who was silent for ' + elapsed_since_last_received + 'ms' );
		ws.close( 1000, "lost connection" );
	});
}

function handleHeartbeatResponse( ws, request, response )
{
	delete ws.last_sent_heartbeat_ts;
	ws.last_sent_heartbeat_ts = null;

	if ( response === 'sleep' )
	{
		//	the peer doesn't want to be bothered with heartbeats any more, but still wants to keep the connection open
		ws.bSleeping = true;
	}

	// as soon as the peer sends a heartbeat himself, we'll think he's woken up and resume our heartbeats too
}

function requestFromLightVendor( command, params, pfnResponseHandler )
{
	if ( ! exports.light_vendor_url )
	{
		_log.consoleLog( "light_vendor_url not set yet" );
		return setTimeout
		(
			function()
			{
				requestFromLightVendor( command, params, pfnResponseHandler );
			},
			1000
		);
	}

	findOutboundPeerOrConnect
	(
		exports.light_vendor_url,
		function( err, ws )
		{
			if ( err )
			{
				return pfnResponseHandler( null, null, { error : "[connect to light vendor failed]: " + err } );
			}

			//	...
			sendRequest
			(
				ws,
				command,
				params,
				false,
				pfnResponseHandler
			);
		}
	);
}

function printConnectionStatus()
{
	_log.consoleLog
	(
		m_oWss.arrClients.length + " incoming connections, "
		+ m_arrOutboundPeers.length + " outgoing connections, "
		+ Object.keys( m_oAssocConnectingOutboundWebSockets ).length + " outgoing connections being opened"
	);
}


/**
 *	subcribe data from others
 */
function subscribe( ws )
{
	//
	//	this is to detect self-connect
	//
	ws.subscription_id	= _crypto.randomBytes( 30 ).toString( "base64" );

	//
	//	obtain the value of the last main chain index
	//	from database
	//
	_storage.readLastMainChainIndex
	(
		function( last_mci )
		{
			//
			//	send subscribe request with subscription id and last mci
			//
			sendRequest
			(
				ws,
				'subscribe',
				{
					subscription_id	: ws.subscription_id,
					last_mci	: last_mci
				},
				false,
				function( ws, request, response )
				{
					delete ws.subscription_id;
					ws.subscription_id = null;

					if ( response.error )
					{
						_log.consoleLog( "network::subscribe, occurred a error: ", response.error );
						return;
					}

					//
					//	emit event connected_to_source to tell all listeners that:
					//	we have connected to source successfully
					//	and, now start to do what you should do.
					//
					ws.bSource	= true;
					_event_bus.emit
					(
						'connected_to_source',
						ws
					);
				}
			);
		}
	);
}







//////////////////////////////////////////////////////////////////////
//	joints
//////////////////////////////////////////////////////////////////////

/**
 *	sent as justsaying or as response to a request
 */
function sendJoint( ws, objJoint, tag )
{
	_log.consoleLog( 'sending joint identified by unit ' + objJoint.unit.unit + ' to', ws.peer );

	//	...
	tag
		?
		sendResponse( ws, tag, { joint : objJoint } )
		:
		sendJustSaying( ws, 'joint', objJoint );
}

/**
 *	sent by light clients to their vendors
 */
function postJointToLightVendor( objJoint, handleResponse )
{
	_log.consoleLog( 'posing joint identified by unit ' + objJoint.unit.unit + ' to light vendor' );
	requestFromLightVendor
	(
		'post_joint',
		objJoint,
		function( ws, request, response )
		{
			handleResponse( response );
		}
	);
}

function sendFreeJoints( ws )
{
	_storage.readFreeJoints
	(
		function( objJoint )
		{
			sendJoint( ws, objJoint );
		},
		function()
		{
			sendJustSaying( ws, 'free_joints_end', null );
		}
	);
}

function sendJointsSinceMci( ws, mci )
{
	_joint_storage.readJointsSinceMci
	(
		mci, 
		function( objJoint )
		{
			sendJoint( ws, objJoint );
		},
		function()
		{
			sendJustSaying( ws, 'free_joints_end', null );
		}
	);
}

function requestFreeJointsFromAllOutboundPeers()
{
	var i;

	for ( i = 0; i < m_arrOutboundPeers.length; i ++ )
	{
		sendJustSaying( m_arrOutboundPeers[ i ], 'refresh', null );
	}
}

function requestNewJoints( ws )
{
	_storage.readLastMainChainIndex
	(
		function( last_mci )
		{
			sendJustSaying( ws, 'refresh', last_mci );
		}
	);
}

function rerequestLostJoints()
{
	//	_log.consoleLog("rerequestLostJoints");
	if ( m_bCatchingUp )
	{
		return;
	}

	_joint_storage.findLostJoints
	(
		function( arrUnits )
		{
			_log.consoleLog( "lost units", arrUnits );
			tryFindNextPeer
			(
				null,
				function( ws )
				{
					if ( ! ws )
					{
						return;
					}

					//	...
					_log.consoleLog( "found next peer " + ws.peer );
					requestJoints
					(
						ws,
						arrUnits.filter
						(
							function( unit )
							{
								return ( ! m_oAssocUnitsInWork[ unit ] && ! havePendingJointRequest( unit ) );
							}
						)
					);
				}
			);
		}
	);
}

function requestNewMissingJoints( ws, arrUnits )
{
	var arrNewUnits;

	//	...
	arrNewUnits	= [];
	_async.eachSeries
	(
		arrUnits,
		function( unit, cb )
		{
			if ( m_oAssocUnitsInWork[ unit ] )
			{
				return cb();
			}
			if ( havePendingJointRequest( unit ) )
			{
				_log.consoleLog( "unit " + unit + " was already requested" );
				return cb();
			}

			//	...
			_joint_storage.checkIfNewUnit
			(
				unit,
				{
					ifNew : function()
					{
						arrNewUnits.push( unit );
						cb();
					},
					ifKnown : function()
					{
						//	it has just been handled
						_log.consoleLog( "known" );
						cb();
					},
					ifKnownUnverified : function()
					{
						//	I was already waiting for it
						_log.consoleLog( "known unverified" );
						cb();
					},
					ifKnownBad : function( error )
					{
						throw Error( "known bad " + unit + ": " + error );
					}
				}
			);
		},
		function()
		{
			//
			//	_log.consoleLog(arrNewUnits.length+" of "+arrUnits.length+" left", m_oAssocUnitsInWork);
			//	filter again as something could have changed each time we were paused in checkIfNewUnit
			arrNewUnits	= arrNewUnits.filter
			(
				function( unit )
				{
					return ( ! m_oAssocUnitsInWork[ unit ] && ! havePendingJointRequest( unit ) );
				}
			);

			if ( arrNewUnits.length > 0 )
			{
				requestJoints( ws, arrNewUnits );
			}
		}
	);
}

function requestJoints( ws, arrUnits )
{
	if ( arrUnits.length === 0 )
	{
		return;
	}

	//	...
	arrUnits.forEach
	(
		function( unit )
		{
			var diff;

			if ( m_oAssocRequestedUnits[ unit ] )
			{
				diff	= Date.now() - m_oAssocRequestedUnits[ unit ];

				//	since response handlers are called in nextTick(), there is a period when the pending request is already cleared but the response
				//	handler is not yet called, hence m_oAssocRequestedUnits[unit] not yet cleared
				if ( diff <= _network_consts.STALLED_TIMEOUT )
				{
					return _log.consoleLog( "unit " + unit + " already requested " + diff + " ms ago, m_oAssocUnitsInWork=" + m_oAssocUnitsInWork[ unit ] );
					//	throw new Error("unit "+unit+" already requested "+diff+" ms ago, m_oAssocUnitsInWork="+m_oAssocUnitsInWork[unit]);
				}
			}
			if ( ws.readyState === ws.OPEN )
			{
				m_oAssocRequestedUnits[ unit ] = Date.now();
				// even if readyState is not ws.OPEN, we still send the request, it'll be rerouted after timeout
			}

			//
			//	*
			//	send request for getting joints
			//
			sendRequest
			(
				ws,
				'get_joint',
				unit,
				true,
				handleResponseToJointRequest
			);
		}
	);
}

function handleResponseToJointRequest( ws, request, response )
{
	var unit;
	var objJoint;

	//	...
	delete m_oAssocRequestedUnits[ request.params ];

	if ( ! response.joint )
	{
		//	...
		unit	= request.params;
		if ( response.joint_not_found === unit )
		{
			//	we trust the light vendor that if it doesn't know about the unit after 1 day, it doesn't exist
			if ( _conf.bLight )
			{
				_db.query
				(
					"DELETE FROM unhandled_private_payments WHERE unit=? AND creation_date<" + _db.addTime('-1 DAY'),
					[ unit ]
				);
			}

			//	if it is in unhandled_joints, it'll be deleted in 1 hour
			if ( ! m_bCatchingUp )
			{
				return _log.consoleLog( "unit " + unit + " does not exist" );
			}
			//	return purgeDependenciesAndNotifyPeers(unit, "unit "+unit+" does not exist");

			_db.query
			(
				"SELECT 1 FROM hash_tree_balls WHERE unit=?",
				[ unit ],
				function( rows )
				{
					if ( rows.length === 0 )
					{
						return _log.consoleLog("unit "+unit+" does not exist (catching up)");
						//	return purgeDependenciesAndNotifyPeers(unit, "unit "+unit+" does not exist (catching up)");
					}

					findNextPeer
					(
						ws,
						function( next_ws )
						{
							_breadcrumbs.add( "found next peer to reroute joint_not_found " + unit + ": " + next_ws.peer );
							requestJoints( next_ws, [ unit ] );
						}
					);
				}
			);
		}

		//
		//	if it still exists, we'll request it again
		//	we requst joints in two cases:
		//	- when referenced from parents, in this case we request it from the same peer who sent us the referencing joint,
		//	he should know, or he is attempting to DoS us
		//	- when catching up and requesting old joints from random peers, in this case we are pretty sure it should exist
		//
		return;
	}

	//	...
	objJoint	= response.joint;
	if ( ! objJoint.unit || ! objJoint.unit.unit )
	{
		return sendError(ws, 'no unit');
	}

	//	...
	unit	= objJoint.unit.unit;
	if ( request.params !== unit )
	{
		return sendError( ws, "I didn't request this unit from you: " + unit );
	}

	if ( _conf.bLight && objJoint.ball && ! objJoint.unit.content_hash )
	{
		//	accept it as unfinished (otherwise we would have to require a proof)
		delete objJoint.ball;
		delete objJoint.skiplist_units;
	}

	_conf.bLight ? handleLightOnlineJoint( ws, objJoint ) : handleOnlineJoint( ws, objJoint );
}

function havePendingRequest( command )
{
	var arrPeers;
	var i;
	var assocPendingRequests;
	var tag;

	//	...
	arrPeers = m_oWss.arrClients.concat( m_arrOutboundPeers );

	for ( i = 0; i < arrPeers.length; i++ )
	{
		assocPendingRequests	= arrPeers[ i ].assocPendingRequests;
		for ( tag in assocPendingRequests )
		{
			if ( assocPendingRequests[ tag ].request.command === command )
			{
				return true;
			}
		}
	}

	return false;
}

function havePendingJointRequest( unit )
{
	var arrPeers;
	var i;
	var assocPendingRequests;
	var tag;
	var request;

	//	...
	arrPeers	= m_oWss.arrClients.concat( m_arrOutboundPeers );

	for ( i = 0; i < arrPeers.length; i ++ )
	{
		assocPendingRequests	= arrPeers[ i ].assocPendingRequests;
		for ( tag in assocPendingRequests )
		{
			request	= assocPendingRequests[ tag ].request;
			if ( request.command === 'get_joint' && request.params === unit )
			{
				return true;
			}
		}
	}

	return false;
}

/**
 *	We may receive a reference to a nonexisting unit in parents. We are not going to keep the referencing joint forever.
 */
function purgeJunkUnhandledJoints()
{
	if ( m_bCatchingUp || Date.now() - m_nComingOnlineTime < 3600*1000 )
	{
		return;
	}

	_db.query
	(
		"DELETE FROM unhandled_joints WHERE creation_date < " + _db.addTime( "-1 HOUR" ),
		function()
		{
			_db.query
			(
				"DELETE FROM dependencies WHERE NOT EXISTS (SELECT * FROM unhandled_joints WHERE unhandled_joints.unit=dependencies.unit)"
			);
		}
	);
}

function purgeJointAndDependenciesAndNotifyPeers( objJoint, error, onDone )
{
	if ( error.indexOf( 'is not stable in view of your parents' ) >= 0 )
	{
		//	give it a chance to be retried after adding other units
		_event_bus.emit
		(
			'nonfatal_error',
			"error on unit " + objJoint.unit.unit + ": " + error + "; " + JSON.stringify( objJoint ),
			new Error()
		);
		return onDone();
	}

	_joint_storage.purgeJointAndDependencies
	(
		objJoint, 
		error,
		function( purged_unit, peer )
		{
			//
			//	this callback is called for each dependent unit
			//
			var ws;

			//	...
			ws = getPeerWebSocket( peer );
			if ( ws )
			{
				sendErrorResult( ws, purged_unit, "error on (indirect) parent unit " + objJoint.unit.unit + ": " + error );
			}
		},
		onDone
	);
}

function purgeDependenciesAndNotifyPeers( unit, error, onDone )
{
	_joint_storage.purgeDependencies
	(
		unit, 
		error,
		function( purged_unit, peer )
		{
			//
			//	this callback is called for each dependent unit
			//
			var ws;

			//	...
			ws = getPeerWebSocket( peer );
			if ( ws )
			{
				sendErrorResult( ws, purged_unit, "error on (indirect) parent unit " + unit + ": " + error );
			}
		},
		onDone
	);
}

function forwardJoint( ws, objJoint )
{
	m_oWss.arrClients.concat( m_arrOutboundPeers ).forEach
	(
		function( client )
		{
			if ( client !== ws && client.bSubscribed )
			{
				sendJoint( client, objJoint );
			}
		}
	);
}

function handleJoint( ws, objJoint, bSaved, callbacks )
{
	var unit;
	var validate;

	//	...
	unit = objJoint.unit.unit;

	if ( m_oAssocUnitsInWork[ unit ] )
	{
		return callbacks.ifUnitInWork();
	}

	//	...
	m_oAssocUnitsInWork[ unit ]	= true;
	validate			= function()
	{
		_validation.validate
		(
			objJoint,
			{
				ifUnitError : function( error )
				{
					_log.consoleLog( objJoint.unit.unit + " validation failed: " + error );
					callbacks.ifUnitError( error );
					//	throw Error(error);

					purgeJointAndDependenciesAndNotifyPeers
					(
						objJoint,
						error,
						function()
						{
							delete m_oAssocUnitsInWork[ unit ];
						}
					);

					if ( ws && error !== 'authentifier verification failed' && ! error.match( /bad merkle proof at path/ ) )
					{
						writeEvent('invalid', ws.host);
					}

					if ( objJoint.unsigned )
					{
						_event_bus.emit( "validated-" + unit, false );
					}
				},
				ifJointError : function( error )
				{
					callbacks.ifJointError( error );
					//	throw Error(error);
					_db.query
					(
						"INSERT INTO known_bad_joints (joint, json, error) VALUES (?,?,?)",
						[
							_object_hash.getJointHash( objJoint ),
							JSON.stringify( objJoint ),
							error
						],
						function()
						{
							delete m_oAssocUnitsInWork[ unit ];
						}
					);

					if ( ws )
					{
						writeEvent( 'invalid', ws.host );
					}
					if ( objJoint.unsigned )
					{
						_event_bus.emit( "validated-" + unit, false );
					}
				},
				ifTransientError : function( error )
				{
					//
					//	TODO
					//	memory lack ?
					//
					throw Error( error );
					_log.consoleLog( "############################## transient error " + error );
					delete m_oAssocUnitsInWork[ unit ];
				},
				ifNeedHashTree : function()
				{
					_log.consoleLog('need hash tree for unit '+unit);

					if ( objJoint.unsigned )
					{
						throw Error( "ifNeedHashTree() unsigned" );
					}

					//	...
					callbacks.ifNeedHashTree();

					//	we are not saving unhandled joint because we don't know dependencies
					delete m_oAssocUnitsInWork[ unit ];
				},
				ifNeedParentUnits : callbacks.ifNeedParentUnits,
				ifOk : function( objValidationState, validation_unlock )
				{
					if ( objJoint.unsigned )
					{
						throw Error( "ifOk() unsigned" );
					}

					//	...
					_profilerex.begin( "#saveJoint" );

					_writer.saveJoint
					(
						objJoint,
						objValidationState,
						null,
						function()
						{
							//	...
							_profilerex.end( "#saveJoint" );

							//	...
							validation_unlock();
							callbacks.ifOk();

							if ( ws )
							{
								writeEvent
								(
									( objValidationState.sequence !== 'good' ) ? 'nonserial' : 'new_good',
									ws.host
								);
							}

							notifyWatchers( objJoint, ws );
							if ( ! m_bCatchingUp )
							{
								_event_bus.emit('new_joint', objJoint);
							}
						}
					);
				},
				ifOkUnsigned : function( bSerial )
				{
					if ( ! objJoint.unsigned )
					{
						throw Error( "ifOkUnsigned() signed" );
					}

					//	...
					callbacks.ifOkUnsigned();
					_event_bus.emit
					(
						"validated-" + unit,
						bSerial
					);
				}
			}
		);
	};

	_joint_storage.checkIfNewJoint
	(
		objJoint,
		{
			ifNew : function()
			{
				bSaved ? callbacks.ifNew() : validate();
			},
			ifKnown : function()
			{
				callbacks.ifKnown();
				delete m_oAssocUnitsInWork[ unit ];
			},
			ifKnownBad : function()
			{
				callbacks.ifKnownBad();
				delete m_oAssocUnitsInWork[ unit ];
			},
			ifKnownUnverified : function()
			{
				bSaved ? validate() : callbacks.ifKnownUnverified();
			}
		}
	);
}


/**
 *	handle joint posted to me by a light client
 */
function handlePostedJoint( ws, objJoint, onDone )
{
	var unit;

	if ( ! objJoint || ! objJoint.unit || ! objJoint.unit.unit )
	{
		return onDone( 'no unit' );
	}

	//	...
	unit	= objJoint.unit.unit;
	delete objJoint.unit.main_chain_index;

	handleJoint
	(
		ws,
		objJoint,
		false,
		{
			ifUnitInWork : function()
			{
				onDone("already handling this unit");
			},
			ifUnitError : function( error )
			{
				onDone( error );
			},
			ifJointError : function( error )
			{
				onDone( error );
			},
			ifNeedHashTree : function()
			{
				onDone("need hash tree");
			},
			ifNeedParentUnits : function( arrMissingUnits )
			{
				onDone("unknown parents");
			},
			ifOk : function()
			{
				onDone();

				//	forward to other peers
				if ( ! m_bCatchingUp && ! _conf.bLight )
				{
					forwardJoint( ws, objJoint );
				}

				delete m_oAssocUnitsInWork[ unit ];
			},
			ifOkUnsigned : function()
			{
				delete m_oAssocUnitsInWork[unit];
				onDone( "you can't send unsigned units" );
			},
			ifKnown : function()
			{
				if ( objJoint.unsigned )
				{
					throw Error( "known unsigned" );
				}

				//	...
				onDone( "known" );
				writeEvent( 'known_good', ws.host );
			},
			ifKnownBad : function()
			{
				onDone( "known bad" );
				writeEvent( 'known_bad', ws.host );
			},
			ifKnownUnverified : function()
			{
				//	impossible unless the peer also sends this joint by 'joint' justsaying
				onDone( "known unverified" );
				delete m_oAssocUnitsInWork[ unit ];
			}
		}
	);
}

function handleOnlineJoint( ws, objJoint, onDone )
{
	var unit;

	if ( ! onDone)
	{
		onDone = function(){};
	}

	//	...
	unit = objJoint.unit.unit;
	delete objJoint.unit.main_chain_index;

	//	...
	handleJoint
	(
		ws,
		objJoint,
		false,
		{
			ifUnitInWork : onDone,
			ifUnitError : function( error )
			{
				sendErrorResult( ws, unit, error );
				onDone();
			},
			ifJointError : function( error )
			{
				sendErrorResult( ws, unit, error );
				onDone();
			},
			ifNeedHashTree : function()
			{
				if ( ! m_bCatchingUp && ! m_bWaitingForCatchupChain )
				{
					requestCatchup( ws );
				}

				//
				//	we are not saving the joint so that in case requestCatchup() fails,
				//	the joint will be requested again via findLostJoints,
				//	which will trigger another attempt to request catchup
				//
				onDone();
			},
			ifNeedParentUnits : function( arrMissingUnits )
			{
				sendInfo( ws, { unit : unit, info : "unresolved dependencies: " + arrMissingUnits.join( ", " ) } );
				_joint_storage.saveUnhandledJointAndDependencies( objJoint, arrMissingUnits, ws.peer, function()
				{
					delete m_oAssocUnitsInWork[unit];
				});
				requestNewMissingJoints( ws, arrMissingUnits );
				onDone();
			},
			ifOk : function()
			{
				sendResult( ws, { unit: unit, result: 'accepted' } );

				//	forward to other peers
				if ( ! m_bCatchingUp && ! _conf.bLight )
				{
					forwardJoint( ws, objJoint );
				}

				//	...
				delete m_oAssocUnitsInWork[ unit ];

				//	wake up other joints that depend on me
				findAndHandleJointsThatAreReady( unit );
				onDone();
			},
			ifOkUnsigned : function()
			{
				delete m_oAssocUnitsInWork[ unit ];
				onDone();
			},
			ifKnown : function()
			{
				if ( objJoint.unsigned )
				{
					throw Error( "known unsigned" );
				}

				//	...
				sendResult( ws, { unit: unit, result: 'known' } );
				writeEvent( 'known_good', ws.host );
				onDone();
			},
			ifKnownBad : function()
			{
				sendResult( ws, { unit : unit, result : 'known_bad' } );
				writeEvent( 'known_bad', ws.host );
				if ( objJoint.unsigned )
				{
					_event_bus.emit( "validated-" + unit, false );
				}

				//	...
				onDone();
			},
			ifKnownUnverified : function()
			{
				sendResult( ws, { unit : unit, result : 'known_unverified' } );
				delete m_oAssocUnitsInWork[ unit ];
				onDone();
			}
		}
	);
}

function handleSavedJoint( objJoint, creation_ts, peer )
{
	var unit	= objJoint.unit.unit;
	var ws		= getPeerWebSocket( peer );

	if ( ws && ws.readyState !== ws.OPEN )
	{
		ws = null;
	}

	//	...
	handleJoint
	(
		ws,
		objJoint,
		true,
		{
			ifUnitInWork : function(){},
			ifUnitError : function( error )
			{
				if ( ws )
				{
					sendErrorResult( ws, unit, error );
				}
			},
			ifJointError : function( error )
			{
				if ( ws )
				{
					sendErrorResult( ws, unit, error );
				}
			},
			ifNeedHashTree : function()
			{
				throw Error( "handleSavedJoint: need hash tree" );
			},
			ifNeedParentUnits : function( arrMissingUnits )
			{
				_db.query
				(
					"SELECT 1 FROM archived_joints WHERE unit IN(?) LIMIT 1",
					[
						arrMissingUnits
					],
					function( rows )
					{
						if ( rows.length === 0 )
						{
							throw Error( "unit " + unit + " still has unresolved dependencies: " + arrMissingUnits.join( ", " ) );
						}

						//	...
						_breadcrumbs.add
						(
							"unit " + unit + " has unresolved dependencies that were archived: " + arrMissingUnits.join( ", " )
						);
						if ( ws )
						{
							requestNewMissingJoints( ws, arrMissingUnits );
						}
						else
						{
							findNextPeer
							(
								null,
								function( next_ws )
								{
									requestNewMissingJoints( next_ws, arrMissingUnits );
								}
							);
						}

						//	...
						delete m_oAssocUnitsInWork[ unit ];
					}
				);
			},
			ifOk : function()
			{
				if ( ws )
				{
					sendResult( ws, { unit : unit, result : 'accepted' } );
				}

				//	forward to other peers
				if ( ! m_bCatchingUp && ! _conf.bLight && creation_ts > Date.now() - _network_consts.FORWARDING_TIMEOUT )
				{
					forwardJoint( ws, objJoint );
				}

				_joint_storage.removeUnhandledJointAndDependencies
				(
					unit,
					function()
					{
						delete m_oAssocUnitsInWork[unit];

						//	wake up other saved joints that depend on me
						findAndHandleJointsThatAreReady( unit );
					}
				);
			},
			ifOkUnsigned : function()
			{
				_joint_storage.removeUnhandledJointAndDependencies( unit, function()
				{
					delete m_oAssocUnitsInWork[ unit ];
				}
			);
		},
		//	readDependentJointsThatAreReady can read the same joint twice before it's handled. If not new, just ignore (we've already responded to peer).
		ifKnown : function(){},
		ifKnownBad : function(){},
		ifNew : function()
		{
			//	that's ok: may be simultaneously selected by readDependentJointsThatAreReady and deleted by purgeJunkUnhandledJoints when we wake up after sleep
			delete m_oAssocUnitsInWork[unit];
			_log.consoleLog("new in handleSavedJoint: "+unit);
			//	throw Error("new in handleSavedJoint: "+unit);
		}
	});
}

function handleLightOnlineJoint( ws, objJoint )
{
	//	the lock ensures that we do not overlap with history processing which might also write new joints
	_mutex.lock
	(
		[ "light_joints" ],
		function( unlock )
		{
			_breadcrumbs.add( 'got light_joints for handleLightOnlineJoint ' + objJoint.unit.unit );
			handleOnlineJoint
			(
				ws,
				objJoint,
				function()
				{
					_breadcrumbs.add('handleLightOnlineJoint done');
					unlock();
				}
			);
		}
	);
}

function setWatchedAddresses( _arrWatchedAddresses )
{
	m_arrWatchedAddresses = _arrWatchedAddresses;
}

function addWatchedAddress( address )
{
	m_arrWatchedAddresses.push( address );
}


/**
 *	if any of the watched addresses are affected, notifies:  1. own UI  2. light clients
 */
function notifyWatchers( objJoint, source_ws )
{
	var objUnit;
	var arrAddresses;
	var i;
	var j;
	var message;
	var payload;
	var address;

	//	...
	objUnit		= objJoint.unit;
	arrAddresses	= objUnit.authors.map( function( author )
				{
					return author.address;
				});

	if ( ! objUnit.messages )
	{
		//	voided unit
		return;
	}

	for ( i = 0; i < objUnit.messages.length; i++ )
	{
		message	= objUnit.messages[ i ];
		if ( message.app !== "payment" || ! message.payload )
		{
			continue;
		}

		//	...
		payload	= message.payload;
		for ( j = 0; j < payload.outputs.length; j++ )
		{
			address	= payload.outputs[ j ].address;
			if ( arrAddresses.indexOf( address ) === -1 )
			{
				arrAddresses.push( address );
			}
		}
	}

	//
	//	_.intersection( [ arrays ] )
	//	Creates an array of unique values that are included in all given arrays using SameValueZero for equality comparisons.
	//	The order and references of result values are determined by the first array.
	//
	if ( _.intersection( m_arrWatchedAddresses, arrAddresses ).length > 0 )
	{
		//
		//	Array m_arrWatchedAddresses and arrAddresses both contain unique values.
		//
		_event_bus.emit( "new_my_transactions", [ objJoint.unit.unit ] );
		_event_bus.emit( "new_my_unit-" + objJoint.unit.unit, objJoint );
	}
	else
	{
		_db.query
		(
			"SELECT 1 FROM my_addresses WHERE address IN( ? ) \
			UNION \
			SELECT 1 FROM shared_addresses WHERE shared_address IN( ? )",
			[
				arrAddresses,
				arrAddresses
			],
			function( rows )
			{
				if ( rows.length > 0 )
				{
					_event_bus.emit( "new_my_transactions", [ objJoint.unit.unit ] );
					_event_bus.emit( "new_my_unit-" + objJoint.unit.unit, objJoint );
				}
			}
		);
	}

	if ( _conf.bLight )
	{
		return;
	}

	if ( objJoint.ball )
	{
		//	already stable, light clients will require a proof
		return;
	}

	//
	//	this is a new unstable joint,
	//	light clients will accept it without proof
	//
	_db.query
	(
		"SELECT peer FROM watched_light_addresses WHERE address IN( ? )",
		[
			arrAddresses
		],
		function( rows )
		{
			if ( rows.length === 0 )
			{
				return;
			}

			//	...
			objUnit.timestamp	= Math.round( Date.now() / 1000 );	//	light clients need timestamp
			rows.forEach
			(
				function( row )
				{
					var ws;

					//	...
					ws = getPeerWebSocket( row.peer );
					if ( ws && ws.readyState === ws.OPEN && ws !== source_ws )
					{
						sendJoint( ws, objJoint );
					}
				}
			);
		}
	);
}


function notifyWatchersAboutStableJoints( mci )
{
	//
	//	the event was emitted from inside mysql transaction, make sure it completes so that the changes are visible
	//	If the mci became stable in determineIfStableInLaterUnitsAndUpdateStableMcFlag (rare), write lock is released before the validation commits,
	//	so we might not see this mci as stable yet. Hopefully, it'll complete before light/have_updates roundtrip
	//
	_mutex.lock
	(
		[ "write" ],
		function( unlock )
		{
			//
			//	we don't need to block writes,
			//	we requested the lock just to wait that the current write completes
			//
			unlock();

			//	...
			notifyLocalWatchedAddressesAboutStableJoints( mci );
			_log.consoleLog( "notifyWatchersAboutStableJoints " + mci );

			if ( mci <= 1 )
			{
				return;
			}

			//
			//	find last ball ...
			//
			_storage.findLastBallMciOfMci
			(
				_db,
				mci,
				function( last_ball_mci )
				{
					_storage.findLastBallMciOfMci
					(
						_db,
						mci - 1,
						function( prev_last_ball_mci )
						{
							if ( prev_last_ball_mci === last_ball_mci )
							{
								return;
							}

							//	...
							notifyLightClientsAboutStableJoints( prev_last_ball_mci, last_ball_mci );
						}
					);
				}
			);
		}
	);
}


/**
 *	from_mci is non-inclusive, to_mci is inclusive
 *	@param	from_mci
 *	@param	to_mci
 */
function notifyLightClientsAboutStableJoints( from_mci, to_mci )
{
	//
	//	watched_light_addresses
	//	stored all light wallets connected to this hub
	//
	_db.query
	(
		"SELECT peer FROM units JOIN unit_authors USING(unit) JOIN watched_light_addresses USING(address) \n\
		WHERE main_chain_index>? AND main_chain_index<=? \n\
		UNION \n\
		SELECT peer FROM units JOIN outputs USING(unit) JOIN watched_light_addresses USING(address) \n\
		WHERE main_chain_index>? AND main_chain_index<=? \n\
		UNION \n\
		SELECT peer FROM units JOIN watched_light_units USING(unit) \n\
		WHERE main_chain_index>? AND main_chain_index<=?",
		[
			from_mci, to_mci,
			from_mci, to_mci,
			from_mci, to_mci
		],
		function( rows )
		{
			rows.forEach
			(
				function( row )
				{
					var ws;

					//	...
					ws = getPeerWebSocket( row.peer );
					if ( ws && ws.readyState === ws.OPEN )
					{
						sendJustSaying( ws, 'light/have_updates' );
					}
				}
			);
			_db.query
			(
				"DELETE FROM watched_light_units \n\
					WHERE unit IN (SELECT unit FROM units WHERE main_chain_index > ? AND main_chain_index<=?)",
				[ from_mci, to_mci ],
				function()
				{
				}
			);
		}
	);
}


function notifyLocalWatchedAddressesAboutStableJoints( mci )
{
	function handleRows( rows )
	{
		if ( rows.length > 0 )
		{
			_event_bus.emit
			(
				'my_transactions_became_stable',
				rows.map
				(
					function( row )
					{
						return row.unit;
					}
				)
			);
			rows.forEach
			(
				function( row )
				{
					_event_bus.emit( 'my_stable-' + row.unit );
					_log.consoleLog( "notifyLocalWatchedAddressesAboutStableJoints, handleRows, emit event: my_stable-" + row.unit );
				}
			);
		}
	}

	if ( m_arrWatchedAddresses.length > 0 )
	{
		_db.query
		(
			"SELECT unit FROM units JOIN unit_authors USING(unit) WHERE main_chain_index = ? AND address IN( ? ) AND sequence='good' \n\
			UNION \n\
			SELECT unit FROM units JOIN outputs USING(unit) WHERE main_chain_index = ? AND address IN( ? ) AND sequence='good'",
			[
				mci,
				m_arrWatchedAddresses,
				mci,
				m_arrWatchedAddresses
			],
			handleRows
		);
	}

	//
	//	TODO
	//	multi-table UNION ?
	//
	_db.query
	(
		"SELECT unit FROM units JOIN unit_authors USING(unit) JOIN my_addresses USING(address) WHERE main_chain_index=? AND sequence='good' \n\
		UNION \n\
		SELECT unit FROM units JOIN outputs USING(unit) JOIN my_addresses USING(address) WHERE main_chain_index=? AND sequence='good' \n\
		UNION \n\
		SELECT unit FROM units JOIN unit_authors USING(unit) JOIN shared_addresses ON address=shared_address WHERE main_chain_index=? AND sequence='good' \n\
		UNION \n\
		SELECT unit FROM units JOIN outputs USING(unit) JOIN shared_addresses ON address=shared_address WHERE main_chain_index=? AND sequence='good'",
		[
			mci,
			mci,
			mci,
			mci
		],
		handleRows
	);
}


function addLightWatchedAddress( address )
{
	if ( ! _conf.bLight || ! exports.light_vendor_url)
	{
		return;
	}

	//	...
	findOutboundPeerOrConnect
	(
		exports.light_vendor_url,
		function( err, ws )
		{
			if ( err )
			{
				return;
			}

			//	...
			sendJustSaying( ws, 'light/new_address_to_watch', address );
		}
	);
}


/**
 *
 *	@param forceFlushing
 */
function flushEvents( forceFlushing )
{
	var arrQueryParams;
	var objUpdatedHosts;
	var host;
	var columns_obj;
	var sql_columns_updates;
	var column;

	if ( m_arrPeerEventsBuffer.length === 0 || ( ! forceFlushing && m_arrPeerEventsBuffer.length !== 100 ) )
	{
		return;
	}

	//	...
	arrQueryParams	= [];
	objUpdatedHosts	= {};

	//	...
	m_arrPeerEventsBuffer.forEach( function( event_row )
	{
		var host;
		var event;
		var event_date;
		var column;

		//	...
		host		= event_row.host;
		event		= event_row.event;
		event_date	= event_row.event_date;

		if ( event === 'new_good' )
		{
			//
			//	increase value by step 1
			//	objUpdatedHosts.host.column ++
			//
			column	= "count_" + event + "_joints";
			_.set
			(
				objUpdatedHosts,
				[
					host,
					column
				],
				_.get( objUpdatedHosts, [ host, column ], 0 ) + 1
			);
		}

		arrQueryParams.push
		(
			"(" + _db.escape( host ) + "," + _db.escape( event ) + "," + _db.getFromUnixTime( event_date ) + ")"
		);
	});

	//	...
	for ( host in objUpdatedHosts )
	{
		columns_obj		= objUpdatedHosts[ host ];
		sql_columns_updates	= [];

		for ( column in columns_obj )
		{
			sql_columns_updates.push( column + "=" + column + "+" + columns_obj[ column ] );
		}
		_db.query
		(
			"UPDATE peer_hosts SET " + sql_columns_updates.join() + " WHERE peer_host=?",
			[ host ]
		);
	}

	_db.query
	(
		"INSERT INTO peer_events ( peer_host, event, event_date ) VALUES " + arrQueryParams.join()
	);

	m_arrPeerEventsBuffer	= [];
	objUpdatedHosts		= {};
}


function writeEvent( event, host )
{
	var column;
	var event_date;

	if ( _conf.bLight )
	{
		return;
	}

	if ( event === 'invalid' || event === 'nonserial' )
	{
		column	= "count_" + event + "_joints";

		_db.query
		(
			"UPDATE peer_hosts SET " + column + "=" + column + "+1 WHERE peer_host=?",
			[ host ]
		);
		_db.query
		(
			"INSERT INTO peer_events (peer_host, event) VALUES (?,?)",
			[ host, event ]
		);

		return;
	}

	//	...
	event_date	= Math.floor( Date.now() / 1000 );
	m_arrPeerEventsBuffer.push( { host : host, event : event, event_date : event_date } );

	//	...
	flushEvents();
}


function findAndHandleJointsThatAreReady( unit )
{
	_joint_storage.readDependentJointsThatAreReady( unit, handleSavedJoint );
	handleSavedPrivatePayments( unit );
}


function comeOnline()
{
	m_bCatchingUp		= false;
	m_nComingOnlineTime	= Date.now();

	waitTillIdle
	(
		function()
		{
			requestFreeJointsFromAllOutboundPeers();
			setTimeout
			(
				cleanBadSavedPrivatePayments,
				300 * 1000
			);
		}
	);
	_event_bus.emit( 'catching_up_done' );
}


function isIdle()
{
	//	_log.consoleLog(_db._freeConnections.length +"/"+ _db._allConnections.length+" connections are free, "+_mutex.getCountOfQueuedJobs()+" jobs queued, "+_mutex.getCountOfLocks()+" locks held, "+Object.keys(m_oAssocUnitsInWork).length+" units in work");
	return (
		_db.getCountUsedConnections() === 0 &&
		_mutex.getCountOfQueuedJobs() === 0 &&
		_mutex.getCountOfLocks() === 0 &&
		Object.keys( m_oAssocUnitsInWork ).length === 0
	);
}


function waitTillIdle( onIdle )
{
	if ( isIdle() )
	{
		m_bWaitingTillIdle	= false;
		onIdle();
	}
	else
	{
		m_bWaitingTillIdle	= true;
		setTimeout
		(
			function()
			{
				waitTillIdle( onIdle );
			},
			100
		);
	}
}


function broadcastJoint( objJoint )
{
	//	the joint was already posted to light vendor before saving
	if ( _conf.bLight )
	{
		return;
	}

	m_oWss.arrClients.concat( m_arrOutboundPeers ).forEach
	(
		function( client )
		{
			if ( client.bSubscribed )
			{
				sendJoint( client, objJoint );
			}
		}
	);

	//	...
	notifyWatchers( objJoint );
}





//////////////////////////////////////////////////////////////////////
//	catchup
//////////////////////////////////////////////////////////////////////

function checkCatchupLeftovers()
{
	_db.query
	(
		"SELECT 1 FROM hash_tree_balls \n\
		UNION \n\
		SELECT 1 FROM catchup_chain_balls \n\
		LIMIT 1",
		function( rows )
		{
			if ( rows.length === 0 )
			{
				return _log.consoleLog( 'no leftovers' );
			}

			//	...
			_log.consoleLog( 'have catchup leftovers from the previous run' );
			findNextPeer
			(
				null,
				function( ws )
				{
					_log.consoleLog( 'will request leftovers from ' + ws.peer );
					if ( ! m_bCatchingUp && ! m_bWaitingForCatchupChain )
					{
						requestCatchup( ws );
					}
				}
			);
		}
	);
}


function requestCatchup( ws )
{
	_log.consoleLog("will request catchup from "+ws.peer);

	_event_bus.emit( 'catching_up_started' );

	if ( _conf.storage === 'sqlite' )
	{
		_db.query( "PRAGMA cache_size=-200000", function(){} );
	}

	//	...
	_catchup.purgeHandledBallsFromHashTree
	(
		_db,
		function()
		{
			_db.query
			(
				"SELECT hash_tree_balls.unit \n\
				FROM hash_tree_balls LEFT JOIN units USING(unit) \n\
				WHERE units.unit IS NULL ORDER BY ball_index",
				function( tree_rows )
				{
					//	leftovers from previous run
					if ( tree_rows.length > 0 )
					{
						m_bCatchingUp	= true;
						_log.consoleLog("will request balls found in hash tree");

						//	...
						requestNewMissingJoints
						(
							ws,
							tree_rows.map
							(
								function( tree_row )
								{
									return tree_row.unit;
								}
							)
						);

						//	...
						waitTillHashTreeFullyProcessedAndRequestNext( ws );

						//	...
						return;
					}

					//	...
					_db.query
					(
						"SELECT 1 FROM catchup_chain_balls LIMIT 1",
						function( chain_rows )
						{
							//	leftovers from previous run
							if ( chain_rows.length > 0 )
							{
								m_bCatchingUp = true;
								requestNextHashTree( ws );
								return;
							}

							//
							//	we are not switching to catching up mode until we receive a catchup chain
							// 	- don't allow peers to throw us into
							//	catching up mode by just sending a ball

							//	to avoid duplicate requests, we are raising this flag before actually sending the request
							//	(will also reset the flag only after the response is fully processed)
							m_bWaitingForCatchupChain = true;
					
							_log.consoleLog( 'will read last stable mci for catchup' );
							_storage.readLastStableMcIndex
							(
								_db,
								function( last_stable_mci )
								{
									_storage.readLastMainChainIndex
									(
										function( last_known_mci )
										{
											_my_witnesses.readMyWitnesses
											(
												function( arrWitnesses )
												{
													var params =
													{
														witnesses	: arrWitnesses,
														last_stable_mci	: last_stable_mci,
														last_known_mci	: last_known_mci
													};
													sendRequest
													(
														ws,
														'catchup',
														params,
														true,
														handleCatchupChain
													);
												},
												'wait'
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


function handleCatchupChain( ws, request, response )
{
	var catchupChain;

	if ( response.error )
	{
		m_bWaitingForCatchupChain = false;
		_log.consoleLog( 'catchup request got error response: ' + response.error );
		//	findLostJoints will wake up and trigger another attempt to request catchup
		return;
	}

	//	...
	catchupChain = response;
	_catchup.processCatchupChain
	(
		catchupChain,
		ws.peer,
		{
			ifError	: function( error )
			{
				m_bWaitingForCatchupChain = false;
				sendError( ws, error );
			},
			ifOk	: function()
			{
				m_bWaitingForCatchupChain = false;
				m_bCatchingUp = true;
				requestNextHashTree( ws );
			},
			ifCurrent : function()
			{
				m_bWaitingForCatchupChain = false;
			}
		}
	);
}








//////////////////////////////////////////////////////////////////////
//	hash tree
//////////////////////////////////////////////////////////////////////


function requestNextHashTree( ws )
{
	_event_bus.emit( 'catchup_next_hash_tree' );
	_db.query
	(
		"SELECT ball FROM catchup_chain_balls ORDER BY member_index LIMIT 2",
		function( rows )
		{
			var from_ball;
			var to_ball;
			var tag;

			if ( rows.length === 0 )
			{
				return comeOnline();
			}
			if ( rows.length === 1 )
			{
				_db.query
				(
					"DELETE FROM catchup_chain_balls WHERE ball=?",
					[
						rows[ 0 ].ball
					],
					function()
					{
						comeOnline();
					}
				);
				return;
			}

			//	...
			from_ball	= rows[ 0 ].ball;
			to_ball		= rows[ 1 ].ball;

			//	don't send duplicate requests
			for ( tag in ws.assocPendingRequests )
			{
				if ( ws.assocPendingRequests[ tag ].request.command === 'get_hash_tree' )
				{
					_log.consoleLog("already requested hash tree from this peer");
					return;
				}
			}

			//	send request
			sendRequest
			(
				ws,
				'get_hash_tree',
				{
					from_ball	: from_ball,
					to_ball		: to_ball
				},
				true,
				handleHashTree
			);
		}
	);
}


function handleHashTree( ws, request, response )
{
	var hashTree;

	if ( response.error )
	{
		_log.consoleLog( 'get_hash_tree got error response: ' + response.error );
		waitTillHashTreeFullyProcessedAndRequestNext( ws );
		//	after 1 sec, it'll request the same hash tree, likely from another peer
		return;
	}

	//	...
	hashTree	= response;
	_catchup.processHashTree
	(
		hashTree.balls,
		{
			ifError : function( error )
			{
				sendError( ws, error );
				waitTillHashTreeFullyProcessedAndRequestNext( ws );
				//	after 1 sec, it'll request the same hash tree, likely from another peer
			},
			ifOk : function()
			{
				requestNewMissingJoints
				(
					ws,
					hashTree.balls.map
					(
						function( objBall )
						{
							return objBall.unit;
						}
					)
				);
				waitTillHashTreeFullyProcessedAndRequestNext( ws );
			}
		}
	);
}

function waitTillHashTreeFullyProcessedAndRequestNext( ws )
{
	setTimeout( function()
	{
		_db.query
		(
			"SELECT 1 FROM hash_tree_balls LEFT JOIN units USING(unit) WHERE units.unit IS NULL LIMIT 1",
			function( rows )
			{
				if ( rows.length === 0 )
				{
					findNextPeer
					(
						ws,
						function( next_ws )
						{
							requestNextHashTree( next_ws );
						}
					);
				}
				else
				{
					waitTillHashTreeFullyProcessedAndRequestNext( ws );
				}
			}
		);

	}, 1000 );
}







//////////////////////////////////////////////////////////////////////
//	private payments
//////////////////////////////////////////////////////////////////////

function sendPrivatePaymentToWs( ws, arrChains )
{
	//	each chain is sent as separate ws message
	arrChains.forEach( function( arrPrivateElements )
	{
		sendJustSaying( ws, 'private_payment', arrPrivateElements );
	});
}

/**
 *	sends multiple private payloads and their corresponding chains
 */
function sendPrivatePayment( peer, arrChains )
{
	var ws;

	//	...
	ws = getPeerWebSocket( peer );
	if ( ws )
	{
		return sendPrivatePaymentToWs(ws, arrChains);
	}

	//	...
	findOutboundPeerOrConnect( peer, function( err, ws )
	{
		if ( ! err )
		{
			sendPrivatePaymentToWs( ws, arrChains );
		}
	});
}

/**
 * 	handles one private payload and its chain
 */
function handleOnlinePrivatePayment( ws, arrPrivateElements, bViaHub, callbacks )
{
	var unit;
	var message_index;
	var output_index;
	var savePrivatePayment;

	if ( ! _validation_utils.isNonemptyArray( arrPrivateElements ) )
	{
		return callbacks.ifError("private_payment content must be non-empty array");
	}

	//	...
	unit			= arrPrivateElements[ 0 ].unit;
	message_index		= arrPrivateElements[ 0 ].message_index;
	output_index		= arrPrivateElements[ 0 ].payload.denomination ? arrPrivateElements[0].output_index : -1;
	savePrivatePayment	= function( cb )
	{
		//
		//	we may receive the same unit and message index but different output indexes if recipient and cosigner are on the same device.
		//	in this case,
		//	we also receive the same (unit, message_index, output_index) twice - as cosigner and as recipient.
		//	That's why IGNORE.
		//
		_db.query
		(
			"INSERT " + _db.getIgnore()
			+ " INTO unhandled_private_payments "
			+ "( unit, message_index, output_index, json, peer ) "
			+ " VALUES ( ?, ?, ?, ?, ? )",
			[
				unit,
				message_index,
				output_index,
				JSON.stringify( arrPrivateElements ),
				bViaHub ? '' : ws.peer
			],
			//	forget peer if received via hub
			function()
			{
				callbacks.ifQueued();
				if ( cb )
				{
					cb();
				}
			}
		);
	};

	if ( _conf.bLight && arrPrivateElements.length > 1 )
	{
		savePrivatePayment( function()
		{
			updateLinkProofsOfPrivateChain( arrPrivateElements, unit, message_index, output_index );

			//	will request the head element
			rerequestLostJointsOfPrivatePayments();
		});
		return;
	}

	_joint_storage.checkIfNewUnit
	(
		unit,
		{
			ifKnown : function()
			{
				//	m_oAssocUnitsInWork[ unit ] = true;
				_private_payment.validateAndSavePrivatePaymentChain
				(
					arrPrivateElements,
					{
						ifOk : function()
						{
							//	delete m_oAssocUnitsInWork[unit];
							callbacks.ifAccepted( unit );
							_event_bus.emit( "new_my_transactions", [ unit ] );
						},
						ifError : function( error )
						{
							//	delete m_oAssocUnitsInWork[unit];
							callbacks.ifValidationError( unit, error );
						},
						ifWaitingForChain : function()
						{
							savePrivatePayment();
						}
					}
				);
			},
			ifNew : function()
			{
				savePrivatePayment();
				//
				//	if received via hub,
				//		I'm requesting from the same hub,
				// 		thus telling the hub that this unit contains a private payment for me.
				//	It would be better to request missing joints from somebody else
				//
				requestNewMissingJoints( ws, [ unit ] );
			},
			ifKnownUnverified : savePrivatePayment,
			ifKnownBad : function()
			{
				callbacks.ifValidationError( unit, "known bad" );
			}
		}
	);
}

/**
 *	if unit is undefined, find units that are ready
 */
function handleSavedPrivatePayments( unit )
{
	//	if (unit && m_oAssocUnitsInWork[unit])
	//		return;
	_mutex.lock
	(
		[ "saved_private" ],
		function( unlock )
		{
			var sql;

			//	...
			sql = unit
				? "SELECT json, peer, unit, message_index, output_index, linked FROM unhandled_private_payments WHERE unit = " + _db.escape( unit )
				: "SELECT json, peer, unit, message_index, output_index, linked FROM unhandled_private_payments CROSS JOIN units USING( unit )";
			_db.query
			(
				sql,
				function( rows )
				{
					var assocNewUnits;

					if ( rows.length === 0 )
					{
						return unlock();
					}

					//	...
					assocNewUnits	= {};
					_async.each
					(
						//	handle different chains in parallel
						rows,
						function( row, cb )
						{
							var arrPrivateElements;
							var ws;
							var validateAndSave;

							//	...
							arrPrivateElements = JSON.parse( row.json );
							ws = getPeerWebSocket( row.peer );

							if ( ws && ws.readyState !== ws.OPEN )
							{
								ws = null;
							}

							//	...
							validateAndSave = function()
							{
								var objHeadPrivateElement	= arrPrivateElements[ 0 ];
								var payload_hash		= _object_hash.getBase64Hash( objHeadPrivateElement.payload );
								var key				= 'private_payment_validated-' + objHeadPrivateElement.unit + '-' + payload_hash + '-' + row.output_index;

								_private_payment.validateAndSavePrivatePaymentChain
								(
									arrPrivateElements,
									{
										ifOk : function()
										{
											if ( ws )
											{
												sendResult
												(
													ws,
													{
														private_payment_in_unit : row.unit,
														result : 'accepted'
													}
												);
											}

											//	received directly from a peer, not through the hub
											if ( row.peer )
											{
												_event_bus.emit( "new_direct_private_chains", [ arrPrivateElements ] );
											}

											//	...
											assocNewUnits[ row.unit ]	= true;
											deleteHandledPrivateChain
											(
												row.unit,
												row.message_index,
												row.output_index,
												cb
											);
											_log.consoleLog( 'emit ' + key );
											_event_bus.emit( key, true );
										},
										ifError : function( error )
										{
											_log.consoleLog( "validation of priv: " + error );
											//	throw Error(error);

											if ( ws )
											{
												sendResult
												(
													ws,
													{
														private_payment_in_unit : row.unit,
														result : 'error',
														error : error
													}
												);
											}

											//	...
											deleteHandledPrivateChain
											(
												row.unit,
												row.message_index,
												row.output_index,
												cb
											);

											_event_bus.emit( key, false );
										},
										//	light only. Means that chain joints (excluding the head) not downloaded yet or not stable yet
										ifWaitingForChain : function()
										{
											cb();
										}
									}
								);
							};
					
							if ( _conf.bLight && arrPrivateElements.length > 1 && !row.linked )
							{
								updateLinkProofsOfPrivateChain
								(
									arrPrivateElements,
									row.unit,
									row.message_index,
									row.output_index,
									cb,
									validateAndSave
								);
							}
							else
							{
								validateAndSave();
							}
						},
						function()
						{
							var arrNewUnits;

							//	...
							unlock();

							//	...
							arrNewUnits = Object.keys(assocNewUnits);

							if ( arrNewUnits.length > 0 )
							{
								_event_bus.emit( "new_my_transactions", arrNewUnits );
							}
						}
					);
				}
			);
		}
	);
}

function deleteHandledPrivateChain( unit, message_index, output_index, cb )
{
	_db.query
	(
		"DELETE FROM unhandled_private_payments WHERE unit = ? AND message_index = ? AND output_index = ?",
		[
			unit,
			message_index,
			output_index
		],
		function()
		{
			cb();
		}
	);
}

/**
 *	* full only
 */
function cleanBadSavedPrivatePayments()
{
	if ( _conf.bLight || m_bCatchingUp )
	{
		return;
	}

	_db.query
	(
		"SELECT DISTINCT unhandled_private_payments.unit FROM unhandled_private_payments LEFT JOIN units USING( unit ) \n\
		WHERE units.unit IS NULL AND unhandled_private_payments.creation_date<" + _db.addTime( '-1 DAY' ),
		function( rows )
		{
			rows.forEach( function( row )
			{
				_breadcrumbs.add( 'deleting bad saved private payment ' + row.unit );
				_db.query
				(
					"DELETE FROM unhandled_private_payments WHERE unit = ?",
					[
						row.unit
					]
				);
			});
		}
	);
}

/**
 *	* light only
 */
function rerequestLostJointsOfPrivatePayments()
{
	if ( ! _conf.bLight || ! exports.light_vendor_url )
	{
		return;
	}

	//	...
	_db.query
	(
		"SELECT DISTINCT unhandled_private_payments.unit " +
		"FROM unhandled_private_payments LEFT JOIN units USING(unit) " +
		"WHERE units.unit IS NULL",
		function( rows )
		{
			var arrUnits;

			if ( rows.length === 0 )
			{
				return;
			}

			//	...
			arrUnits = rows.map
			(
				function( row )
				{
					return row.unit;
				}
			);

			findOutboundPeerOrConnect
			(
				exports.light_vendor_url,
				function( err, ws )
				{
					if ( err )
					{
						return;
					}

					requestNewMissingJoints( ws, arrUnits );
				}
			);
		}
	);
}

/**
 *	* light only
 */
function requestUnfinishedPastUnitsOfPrivateChains( arrChains, onDone )
{
	if ( ! onDone )
	{
		onDone = function(){};
	}

	//	...
	_private_payment.findUnfinishedPastUnitsOfPrivateChains
	(
		arrChains,
		true,
		function( arrUnits )
		{
			if ( arrUnits.length === 0 )
			{
				return onDone();
			}

			//	...
			_breadcrumbs.add( arrUnits.length + " unfinished past units of private chains" );
			requestHistoryFor( arrUnits, [], onDone );
		}
	);
}

function requestHistoryFor( arrUnits, arrAddresses, onDone )
{
	if ( ! onDone )
	{
		onDone = function(){};
	}

	//	...
	_my_witnesses.readMyWitnesses
	(
		function( arrWitnesses )
		{
			var objHistoryRequest;

			//	...
			objHistoryRequest	= { witnesses : arrWitnesses };

			if ( arrUnits.length )
			{
				objHistoryRequest.requested_joints = arrUnits;
			}
			if ( arrAddresses.length )
			{
				objHistoryRequest.addresses = arrAddresses;
			}

			//	...
			requestFromLightVendor
			(
				'light/get_history',
				objHistoryRequest,
				function( ws, request, response )
				{
					if ( response.error )
					{
						_log.consoleLog( response.error );
						return onDone( response.error );
					}

					_light.processHistory
					(
						response,
						{
							ifError : function( err )
							{
								sendError( ws, err );
								onDone( err );
							},
							ifOk : function()
							{
								onDone();
							}
						}
					);
				}
			);
		},
		'wait'
	);
}

function requestProofsOfJointsIfNewOrUnstable( arrUnits, onDone )
{
	if ( ! onDone )
	{
		onDone = function(){};
	}

	//	...
	_storage.filterNewOrUnstableUnits
	(
		arrUnits,
		function( arrNewOrUnstableUnits )
		{
			if ( arrNewOrUnstableUnits.length === 0 )
				return onDone();

			requestHistoryFor( arrUnits, [], onDone );
		}
	);
}

/**
 *	* light only
 */
function requestUnfinishedPastUnitsOfSavedPrivateElements()
{
	_mutex.lock
	(
		[ 'private_chains' ],
		function( unlock )
		{
			_db.query
			(
				"SELECT json FROM unhandled_private_payments",
				function( rows )
				{
					var arrChains;

					//	...
					_event_bus.emit( 'unhandled_private_payments_left', rows.length );
					if ( rows.length === 0 )
					{
						return unlock();
					}

					_breadcrumbs.add( rows.length + " unhandled private payments" );
					arrChains	= [];

					rows.forEach
					(
						function( row )
						{
							var arrPrivateElements;

							//	...
							arrPrivateElements = JSON.parse( row.json );
							arrChains.push( arrPrivateElements );
						}
					);

					requestUnfinishedPastUnitsOfPrivateChains
					(
						arrChains,
						function onPrivateChainsReceived( err )
						{
							if ( err )
							{
								return unlock();
							}

							handleSavedPrivatePayments();
							setTimeout( unlock, 2000 );
						}
					);
				}
			);
		}
	);
}

/**
 *	* light only
 *
 *	Note that we are leaking to light vendor information about the full chain.
 *	If the light vendor was a party to any previous transaction in this chain, he'll know how much we received.
 */
function checkThatEachChainElementIncludesThePrevious( arrPrivateElements, handleResult )
{
	var arrUnits;

	//	an issue
	if ( arrPrivateElements.length === 1 )
	{
		return handleResult( true );
	}

	//	...
	arrUnits = arrPrivateElements.map
	(
		function( objPrivateElement )
		{
			return objPrivateElement.unit;
		}
	);
	requestFromLightVendor
	(
		'light/get_link_proofs',
		arrUnits,
		function( ws, request, response )
		{
			var arrChain;

			if ( response.error )
			{
				return handleResult( null );	//	undefined result
			}

			//	...
			arrChain = response;
			if ( ! _validation_utils.isNonemptyArray( arrChain ) )
			{
				//	undefined result
				return handleResult( null );
			}

			_light.processLinkProofs
			(
				arrUnits,
				arrChain,
				{
					ifError : function( err )
					{
						//
						//	TODO
						//	memery lack ?
						//
						_log.consoleLog( "linkproof validation failed: " + err );
						throw Error( err );
						handleResult( false );
					},
					ifOk : function()
					{
						_log.consoleLog("linkproof validated ok");
						handleResult( true );
					}
				}
			);
		}
	);
}

/**
 *	* light only
 */
function updateLinkProofsOfPrivateChain( arrPrivateElements, unit, message_index, output_index, onFailure, onSuccess )
{
	if ( ! _conf.bLight )
	{
		throw Error( "not light but updateLinkProofsOfPrivateChain" );
	}
	if ( ! onFailure )
	{
		onFailure = function(){};
	}
	if ( ! onSuccess )
	{
		onSuccess = function(){};
	}

	//	...
	checkThatEachChainElementIncludesThePrevious
	(
		arrPrivateElements,
		function( bLinked )
		{
			if ( bLinked === null )
			{
				return onFailure();
			}
			if ( ! bLinked )
			{
				return deleteHandledPrivateChain( unit, message_index, output_index, onFailure );
			}

			//	the result cannot depend on output_index
			_db.query
			(
				"UPDATE unhandled_private_payments SET linked = 1 WHERE unit = ? AND message_index = ?",
				[
					unit,
					message_index
				],
				function()
				{
					onSuccess();
				}
			);
		}
	);
}


/**
 *	initialize witnesses
 */
function initWitnessesIfNecessary( ws, onDone )
{
	onDone = onDone || function(){};
	_my_witnesses.readMyWitnesses( function( arrWitnesses )
	{
		//	already have witnesses
		if ( arrWitnesses.length > 0 )
		{
			return onDone();
		}

		sendRequest
		(
			ws,
			'get_witnesses',
			null,
			false,
			function( ws, request, arrWitnesses )
			{
				if ( arrWitnesses.error )
				{
					_log.consoleLog( 'get_witnesses returned error: ' + arrWitnesses.error );
					return onDone();
				}

				_my_witnesses.insertWitnesses( arrWitnesses, onDone );
			}
		);

	}, 'ignore');
}







//////////////////////////////////////////////////////////////////////
//	hub
//////////////////////////////////////////////////////////////////////


function sendStoredDeviceMessages( ws, device_address )
{
	_db.query
	(
		"SELECT message_hash, message FROM device_messages WHERE device_address=? ORDER BY creation_date LIMIT 100",
		[
			device_address
		],
		function( rows )
		{
			rows.forEach
			(
				function( row )
				{
					sendJustSaying
					(
						ws,
						'hub/message',
						{
							message_hash	: row.message_hash,
							message		: JSON.parse( row.message )
						}
					);
				}
			);

			//	...
			sendInfo( ws, rows.length + " messages sent" );
			sendJustSaying( ws, 'hub/message_box_status', ( rows.length === 100 ) ? 'has_more' : 'empty' );
		}
	);
}

function version2int( version )
{
	var arr;

	//	...
	arr = version.split( '.' );
	return arr[ 0 ] * 10000 + arr[ 1 ] * 100 + arr[ 2 ] * 1;
}


/**
 * 	switch/case different message types
 */
function handleJustsaying( ws, subject, body )
{
	var echo_string;

	switch ( subject )
	{
		case 'refresh':
			var mci;

			if ( m_bCatchingUp )
			{
				return;
			}

			//	...
			mci = body;
			if ( _validation_utils.isNonnegativeInteger( mci ) )
			{
				return sendJointsSinceMci( ws, mci );
			}
			else
			{
				return sendFreeJoints( ws );
			}

		case 'version':
			if ( ! body )
			{
				return;
			}

			if ( body.protocol_version !== _constants.version )
			{
				sendError( ws, 'Incompatible versions, mine ' + _constants.version + ', yours ' + body.protocol_version );
				ws.close( 1000, 'incompatible versions' );
				return;
			}
			if ( body.alt !== _constants.alt )
			{
				sendError( ws, 'Incompatible alts, mine ' + _constants.alt + ', yours ' + body.alt );
				ws.close( 1000, 'incompatible alts' );
				return;
			}

			//	...
			ws.library_version	= body.library_version;
			if ( typeof ws.library_version === 'string' &&
				version2int( ws.library_version ) < version2int( _constants.minCoreVersion ) )
			{
				ws.old_core = true;
			}

			//	handled elsewhere
			_event_bus.emit( 'peer_version', ws, body );
			break;

		case 'new_version':
			//	a new version is available
			if ( ! body )
			{
				return;
			}
			if ( ws.bLoggingIn || ws.bLoggedIn )
			{
				//	accept from hub only
				_event_bus.emit( 'new_version', ws, body );
			}
			break;

		case 'hub/push_project_number':
			if ( ! body )
			{
				return;
			}
			if ( ws.bLoggingIn || ws.bLoggedIn )
			{
				_event_bus.emit( 'receivedPushProjectNumber', ws, body );
			}
			break;

		case 'bugreport':
			if ( ! body )
			{
				return;
			}
			if ( _conf.ignoreBugreportRegexp &&
				new RegExp( _conf.ignoreBugreportRegexp ).test( body.message + ' ' + body.exception.toString() ) )
			{
				return _log.consoleLog( 'ignoring bugreport' );
			}

			_mail.sendBugEmail( body.message, body.exception );
			break;

		case 'joint':
			var objJoint;

			//	...
			objJoint = body;

			if ( ! objJoint ||
				! objJoint.unit ||
				! objJoint.unit.unit )
			{
				return sendError( ws, 'no unit' );
			}
			if ( objJoint.ball &&
				! _storage.isGenesisUnit( objJoint.unit.unit ) )
			{
				return sendError( ws, 'only requested joint can contain a ball' );
			}
			if ( _conf.bLight &&
				! ws.bLightVendor )
			{
				return sendError( ws, "I'm a light client and you are not my vendor" );
			}

			//	...
			_db.query
			(
				"SELECT 1 FROM archived_joints WHERE unit=? AND reason='uncovered'",
				[
					objJoint.unit.unit
				],
				function( rows )
				{
					//	ignore it as long is it was unsolicited
					if ( rows.length > 0 )
					{
						return sendError( ws, "this unit is already known and archived" );
					}

					//	light clients accept the joint without proof, it'll be saved as unconfirmed (non-stable)
					return _conf.bLight
						? handleLightOnlineJoint( ws, objJoint )
						: handleOnlineJoint( ws, objJoint );
				}
			);

		case 'free_joints_end':
		case 'result':
		case 'info':
		case 'error':
			break;

		case 'private_payment':
			var arrPrivateElements;

			if ( ! body )
			{
				return;
			}

			//	...
			arrPrivateElements = body;
			handleOnlinePrivatePayment
			(
				ws,
				arrPrivateElements,
				false,
				{
					ifError : function( error )
					{
						sendError( ws, error );
					},
					ifAccepted : function( unit )
					{
						sendResult
						(
							ws,
							{
								private_payment_in_unit : unit,
								result : 'accepted'
							}
						);

						_event_bus.emit( "new_direct_private_chains", [ arrPrivateElements ] );
					},
					ifValidationError : function( unit, error )
					{
						sendResult
						(
							ws,
							{
								private_payment_in_unit : unit,
								result : 'error',
								error : error
							}
						);
					},
					ifQueued : function()
					{
					}
				}
			);
			break;

		case 'my_url':
			var url;

			if ( ! body )
			{
				return;
			}

			//	...
			url	= body;
			if ( ws.bOutbound )
			{
				//	ignore: if you are outbound, I already know your url
				break;
			}

			//	inbound only
			if ( ws.bAdvertisedOwnUrl )
			{
				//	allow it only once per connection
				break;
			}

			//	...
			ws.bAdvertisedOwnUrl	= true;
			if ( url.indexOf( 'ws://' ) !== 0 && url.indexOf( 'wss://' ) !== 0 )
			{
				// invalid url
				break;
			}

			//	...
			ws.claimed_url	= url;
			_db.query
			(
				"SELECT creation_date AS latest_url_change_date, url \
				FROM peer_host_urls \
				WHERE peer_host=? \
				ORDER BY creation_date DESC \
				LIMIT 1",
				[
					ws.host
				],
				function( rows )
				{
					var latest_change;

					//	...
					latest_change	= rows[ 0 ];
					if ( latest_change &&
						latest_change.url === url )
					{
						//	advertises the same url
						return;
					}

					//
					//	var elapsed_time = Date.now() - Date.parse(latest_change.latest_url_change_date);
					//	if (elapsed_time < 24*3600*1000) // change allowed no more often than once per day
					//		return;

					//
					//	verify it is really your url by connecting to this url,
					//	sending a random string through this new connection,
					//	and expecting this same string over existing inbound connection
					//
					ws.sent_echo_string	= _crypto.randomBytes( 30 ).toString( "base64" );
					findOutboundPeerOrConnect
					(
						url,
						function( err, reverse_ws )
						{
							if ( ! err )
							{
								sendJustSaying
								(
									reverse_ws,
									'want_echo',
									ws.sent_echo_string
								);
							}
						}
					);
				}
			);
			break;

		case 'want_echo':
			var reverse_ws;

			//	...
			echo_string	= body;
			if ( ws.bOutbound || ! echo_string )
			{
				// ignore
				break;
			}

			//	inbound only
			if ( ! ws.claimed_url )
			{
				break;
			}

			//	...
			reverse_ws = getOutboundPeerWsByUrl( ws.claimed_url );
			if ( ! reverse_ws )
			{
				//	no reverse outbound connection
				break;
			}

			//	...
			sendJustSaying( reverse_ws, 'your_echo', echo_string );
			break;

		case 'your_echo':
			//	comes on the same ws as my_url, claimed_url is already set
			var outbound_host;
			var arrQueries;

			//	...
			echo_string = body;

			if ( ws.bOutbound || ! echo_string )
			{
				// ignore
				break;
			}

			//	inbound only
			if ( ! ws.claimed_url )
			{
				break;
			}
			if ( ws.sent_echo_string !== echo_string )
			{
				break;
			}

			//	...
			outbound_host	= getHostByPeer( ws.claimed_url );
			arrQueries	= [];

			_db.addQuery
			(
				arrQueries,
				"INSERT " + _db.getIgnore() + " INTO peer_hosts (peer_host) VALUES (?)",
				[
					outbound_host
				]
			);
			_db.addQuery
			(
				arrQueries,
				"INSERT " + _db.getIgnore() + " INTO peers (peer_host, peer, learnt_from_peer_host) VALUES (?,?,?)",
				[
					outbound_host,
					ws.claimed_url,
					ws.host
				]
			);
			_db.addQuery
			(
				arrQueries,
				"UPDATE peer_host_urls SET is_active=NULL, revocation_date=" + _db.getNow() + " WHERE peer_host=?",
				[
					ws.host
				]
			);
			_db.addQuery
			(
				arrQueries,
				"INSERT INTO peer_host_urls (peer_host, url) VALUES (?,?)",
				[
					ws.host,
					ws.claimed_url
				]
			);

			_async.series
			(
				arrQueries
			);
			ws.sent_echo_string = null;
			break;

		case 'hub/login':
			// I'm a hub, the peer wants to authenticate
			var objLogin;
			var finishLogin;

			if ( ! body )
			{
				return;
			}
			if ( ! _conf.bServeAsHub )
			{
				return sendError( ws, "I'm not a hub" );
			}

			//	...
			objLogin = body;

			if ( objLogin.challenge !== ws.challenge )
			{
				return sendError( ws, "wrong challenge" );
			}
			if ( ! objLogin.pubkey ||
				! objLogin.signature )
			{
				return sendError( ws, "no login params" );
			}
			if ( objLogin.pubkey.length !== _constants.PUBKEY_LENGTH )
			{
				return sendError( ws, "wrong pubkey length" );
			}
			if ( objLogin.signature.length !== _constants.SIG_LENGTH )
			{
				return sendError( ws, "wrong signature length" );
			}
			if ( ! _ecdsa_sig.verify( _object_hash.getDeviceMessageHashToSign( objLogin ), objLogin.signature, objLogin.pubkey ) )
			{
				return sendError( ws, "wrong signature" );
			}

			//	...
			ws.device_address	= _object_hash.getDeviceAddress( objLogin.pubkey );

			//	after this point the device is authenticated and can send further commands
			finishLogin = function()
			{
				ws.bLoginComplete	= true;
				if ( ws.onLoginComplete )
				{
					ws.onLoginComplete();
					delete ws.onLoginComplete;
				}
			};

			_db.query
			(
				"SELECT 1 FROM devices WHERE device_address=?",
				[
					ws.device_address
				],
				function( rows )
				{
					if ( rows.length === 0 )
					{
						_db.query
						(
							"INSERT INTO devices ( device_address, pubkey ) VALUES ( ?, ? )",
							[
								ws.device_address,
								objLogin.pubkey
							],
							function()
							{
								sendInfo( ws, "address created" );
								finishLogin();
							}
						);
					}
					else
					{
						sendStoredDeviceMessages( ws, ws.device_address );
						finishLogin();
					}
				}
			);

			if ( _conf.pushApiProjectNumber && _conf.pushApiKey )
			{
				sendJustSaying
				(
					ws,
					'hub/push_project_number',
					{
						projectNumber : _conf.pushApiProjectNumber
					}
				);
			}
			else
			{
				sendJustSaying
				(
					ws,
					'hub/push_project_number',
					{
						projectNumber : 0
					}
				);
			}

			//	...
			_event_bus.emit( 'client_logged_in', ws );
			break;

		case 'hub/refresh':
			//	I'm a hub, the peer wants to download new messages
			if ( ! _conf.bServeAsHub )
			{
				return sendError( ws, "I'm not a hub" );
			}
			if ( ! ws.device_address )
			{
				return sendError( ws, "please log in first" );
			}

			//	...
			sendStoredDeviceMessages( ws, ws.device_address );
			break;

		case 'hub/delete':
			//	I'm a hub, the peer wants to remove a message that he's just handled
			var message_hash;

			if ( ! _conf.bServeAsHub )
			{
				return sendError( ws, "I'm not a hub" );
			}

			//	...
			message_hash = body;
			if ( ! message_hash )
			{
				return sendError( ws, "no message hash" );
			}
			if ( ! ws.device_address )
			{
				return sendError( ws, "please log in first" );
			}

			//	...
			_db.query
			(
				"DELETE FROM device_messages WHERE device_address = ? AND message_hash = ?",
				[
					ws.device_address,
					message_hash
				],
				function()
				{
					sendInfo( ws, "deleted message " + message_hash );
				}
			);
			break;

		//
		//	I'm connected to a hub
		//
		case 'hub/challenge':
		case 'hub/message':
		case 'hub/message_box_status':
			if ( ! body )
			{
				return;
			}

			//	...
			_event_bus.emit( "message_from_hub", ws, subject, body );
			break;

		//	I'm light client
		case 'light/have_updates':
			if ( ! _conf.bLight )
			{
				return sendError( ws, "I'm not light" );
			}
			if ( ! ws.bLightVendor )
			{
				return sendError( ws, "You are not my light vendor" );
			}

			//	...
			_event_bus.emit( "message_for_light", ws, subject, body );
			break;

		//	I'm light vendor
		case 'light/new_address_to_watch':
			var address;

			if ( _conf.bLight )
			{
				return sendError( ws, "I'm light myself, can't serve you" );
			}
			if ( ws.bOutbound )
			{
				return sendError( ws, "light clients have to be inbound" );
			}

			//	...
			address = body;
			if ( ! _validation_utils.isValidAddress( address ) )
			{
				return sendError( ws, "address not valid" );
			}

			_db.query
			(
				"INSERT " + _db.getIgnore() + " INTO watched_light_addresses ( peer, address ) VALUES ( ?, ? )",
				[
					ws.peer,
					address
				],
				function()
				{
					sendInfo( ws, "now watching " + address );

					//	check if we already have something on this address
					_db.query
					(
						"SELECT unit, is_stable FROM unit_authors JOIN units USING(unit) WHERE address=? \n\
						UNION \n\
						SELECT unit, is_stable FROM outputs JOIN units USING(unit) WHERE address=? \n\
						ORDER BY is_stable LIMIT 10",
						[
							address,
							address
						],
						function( rows )
						{
							if ( rows.length === 0 )
							{
								return;
							}
							if ( rows.length === 10 || rows.some( function( row ){ return row.is_stable; } ) )
							{
								sendJustSaying( ws, 'light/have_updates' );
							}

							//	...
							rows.forEach
							(
								function( row )
								{
									if ( row.is_stable )
									{
										return;
									}

									//	...
									_storage.readJoint
									(
										_db,
										row.unit,
										{
											ifFound : function( objJoint )
											{
												sendJoint( ws, objJoint );
											},
											ifNotFound : function()
											{
												throw Error( "watched unit " + row.unit + " not found" );
											}
										}
									);
								}
							);
						}
					);
				}
			);
			break;

		case 'exchange_rates':
			if ( ! ws.bLoggingIn &&
				! ws.bLoggedIn )
			{
				//	accept from hub only
				return;
			}

			//	...
			_.assign( m_exchangeRates, body );
			_event_bus.emit( 'rates_updated' );
			break;

		case 'upgrade_required':
			if ( ! ws.bLoggingIn &&
				! ws.bLoggedIn )
			{
				//	accept from hub only
				return;
			}

			throw Error( "Mandatory upgrade required, please check the release notes at https://github.com/byteball/byteball/releases and upgrade." );
			break;
	}
}


function handleRequest( ws, tag, command, params )
{
	//
	//	ignore repeated request while still preparing response to a previous identical request
	//
	if ( ws.assocInPreparingResponse[ tag ] )
	{
		return _log.consoleLog( "ignoring identical " + command + " request" );
	}

	//	...
	ws.assocInPreparingResponse[ tag ] = true;
	switch ( command )
	{
		case 'heartbeat':
			var bPaused;

			//	the peer is sending heartbeats, therefore he is awake
			ws.bSleeping = false;

			//
			//	true if our timers were paused
			//	Happens only on android, which suspends timers when the app becomes paused but still keeps network connections
			//	Handling 'pause' event would've been more straightforward but with preference KeepRunning=false,
			// 	the event is delayed till resume
			//
			bPaused = (
				typeof window !== 'undefined' &&
				window
				&&
				window.cordova &&
				Date.now() - m_nLastHearbeatWakeTs > _network_consts.PAUSE_TIMEOUT
			);
			if ( bPaused )
			{
				//	opt out of receiving heartbeats and move the connection into a sleeping state
				return sendResponse( ws, tag, 'sleep' );
			}

			//	...
			sendResponse( ws, tag );
			break;

		case 'subscribe':
			var subscription_id;

			if ( ! _validation_utils.isNonemptyObject( params ) )
			{
				return sendErrorResponse(ws, tag, 'no params');
			}

			//	...
			subscription_id = params.subscription_id;

			if ( typeof subscription_id !== 'string' )
			{
				return sendErrorResponse( ws, tag, 'no subscription_id' );
			}

			if ( m_oWss.arrClients.concat( m_arrOutboundPeers ).some( function( other_ws ){ return ( other_ws.subscription_id === subscription_id ); } ) )
			{
				if ( ws.bOutbound )
				{
					_db.query
					(
						"UPDATE peers SET is_self = 1 WHERE peer = ?",
						[
							ws.peer
						]
					);
				}

				//	...
				sendErrorResponse( ws, tag, "self-connect" );
				return ws.close( 1000, "self-connect" );
			}

			if ( _conf.bLight )
			{
				//	if (ws.peer === exports.light_vendor_url)
				//		sendFreeJoints(ws);
				return sendErrorResponse( ws, tag, "I'm light, cannot subscribe you to updates" );
			}
			if ( ws.old_core )
			{
				sendJustSaying( ws, 'upgrade_required' );
				sendErrorResponse( ws, tag, "old core" );
				return ws.close( 1000, "old core" );
			}

			//	...
			ws.bSubscribed = true;
			sendResponse( ws, tag, "subscribed" );
			if ( m_bCatchingUp )
			{
				return;
			}

			if ( _validation_utils.isNonnegativeInteger( params.last_mci ) )
			{
				sendJointsSinceMci( ws, params.last_mci );
			}
			else
			{
				sendFreeJoints( ws );
			}

			break;
			
		case 'get_joint':
			//	peer needs a specific joint
			var unit;

			//	if ( m_bCatchingUp )
			//		return;
			if ( ws.old_core )
			{
				return sendErrorResponse( ws, tag, "old core, will not serve get_joint" );
			}

			//	...
			unit = params;
			_storage.readJoint
			(
				_db,
				unit,
				{
					ifFound : function( objJoint )
					{
						sendJoint( ws, objJoint, tag );
					},
					ifNotFound : function()
					{
						sendResponse( ws, tag, { joint_not_found : unit } );
					}
				}
			);
			break;
			
		case 'post_joint':
			//	only light clients use this command to post joints they created
			var objJoint;

			//	...
			objJoint = params;
			handlePostedJoint
			(
				ws,
				objJoint,
				function( error )
				{
					error
						? sendErrorResponse( ws, tag, error )
						: sendResponse( ws, tag, 'accepted' );
				}
			);
			break;
			
		case 'catchup':
			var catchupRequest;

			if ( ! ws.bSubscribed )
			{
				return sendErrorResponse(ws, tag, "not subscribed, will not serve catchup");
			}

			//	...
			catchupRequest = params;
			_mutex.lock
			(
				[ 'catchup_request' ],
				function( unlock )
				{
					if ( ! ws || ws.readyState !== ws.OPEN )
					{
						//	may be already gone when we receive the lock
						return process.nextTick( unlock );
					}

					_catchup.prepareCatchupChain
					(
						catchupRequest,
						{
							ifError : function( error )
							{
								sendErrorResponse( ws, tag, error );
								unlock();
							},
							ifOk : function( objCatchupChain )
							{
								sendResponse( ws, tag, objCatchupChain );
								unlock();
							}
						}
					);
				}
			);
			break;

		case 'get_hash_tree':
			var hashTreeRequest;

			if ( ! ws.bSubscribed )
			{
				return sendErrorResponse( ws, tag, "not subscribed, will not serve get_hash_tree" );
			}

			//	...
			hashTreeRequest	= params;
			_mutex.lock
			(
				[ 'get_hash_tree_request' ],
				function( unlock )
				{
					if ( ! ws || ws.readyState !== ws.OPEN )
					{
						//	may be already gone when we receive the lock
						return process.nextTick(unlock);
					}

					_catchup.readHashTree
					(
						hashTreeRequest,
						{
							ifError : function( error )
							{
								sendErrorResponse( ws, tag, error );
								unlock();
							},
							ifOk : function( arrBalls )
							{
								//
								//	we have to wrap arrBalls into an object because the peer
								//	will check .error property first
								//
								sendResponse( ws, tag, { balls: arrBalls } );
								unlock();
							}
						}
					);
				}
			);
			break;

		case 'get_peers':
			var arrPeerUrls;

			//	...
			arrPeerUrls	= m_arrOutboundPeers.map( function( ws ){ return ws.peer; } );
			//	empty array is ok
			sendResponse( ws, tag, arrPeerUrls );
			break;
			
		case 'get_witnesses':
			_my_witnesses.readMyWitnesses
			(
				function( arrWitnesses )
				{
					sendResponse( ws, tag, arrWitnesses );
				},
				'wait'
			);
			break;

		case 'get_last_mci':
			_storage.readLastMainChainIndex
			(
				function( last_mci )
				{
					sendResponse( ws, tag, last_mci );
				}
			);
			break;

		//	I'm a hub, the peer wants to deliver a message to one of my clients
		case 'hub/deliver':
			var objDeviceMessage;
			var bToMe;

			//	...
			objDeviceMessage = params;

			if ( ! objDeviceMessage || ! objDeviceMessage.signature || ! objDeviceMessage.pubkey || ! objDeviceMessage.to
					|| ! objDeviceMessage.encrypted_package || ! objDeviceMessage.encrypted_package.dh
					|| ! objDeviceMessage.encrypted_package.dh.sender_ephemeral_pubkey
					|| ! objDeviceMessage.encrypted_package.encrypted_message
					|| ! objDeviceMessage.encrypted_package.iv || ! objDeviceMessage.encrypted_package.authtag )
			{
				return sendErrorResponse(ws, tag, "missing fields");
			}

			//	...
			bToMe	= ( m_sMyDeviceAddress && m_sMyDeviceAddress === objDeviceMessage.to );
			if ( ! _conf.bServeAsHub && ! bToMe )
			{
				return sendErrorResponse( ws, tag, "I'm not a hub" );
			}
			if ( ! _ecdsa_sig.verify( _object_hash.getDeviceMessageHashToSign( objDeviceMessage ), objDeviceMessage.signature, objDeviceMessage.pubkey ) )
			{
				return sendErrorResponse(ws, tag, "wrong message signature");
			}

			//	if i'm always online and i'm my own hub
			if ( bToMe )
			{
				sendResponse( ws, tag, "accepted" );
				_event_bus.emit
				(
					"message_from_hub",
					ws,
					'hub/message',
					{
						message_hash : _object_hash.getBase64Hash( objDeviceMessage ),
						message : objDeviceMessage
					}
				);
				return;
			}

			_db.query
			(
				"SELECT 1 FROM devices WHERE device_address=?",
				[ objDeviceMessage.to ],
				function( rows )
				{
					var message_hash;

					if ( rows.length === 0 )
					{
						return sendErrorResponse( ws, tag, "address " + objDeviceMessage.to + " not registered here" );
					}

					//	...
					message_hash = _object_hash.getBase64Hash( objDeviceMessage );

					_db.query
					(
						"INSERT " + _db.getIgnore() + " INTO device_messages (message_hash, message, device_address) VALUES (?,?,?)",
						[
							message_hash,
							JSON.stringify( objDeviceMessage ),
							objDeviceMessage.to
						],
						function()
						{
							//	if the addressee is connected, deliver immediately
							m_oWss.arrClients.forEach
							(
								function( client )
								{
									if ( client.device_address === objDeviceMessage.to )
									{
										sendJustSaying
										(
											client,
											'hub/message',
											{
												message_hash	: message_hash,
												message		: objDeviceMessage
											}
										);
									}
								}
							);

							sendResponse( ws, tag, "accepted" );
							_event_bus.emit( 'peer_sent_new_message', ws, objDeviceMessage );
						}
					);
				}
			);
			break;

		//
		//	I'm a hub, the peer wants to get a correspondent's temporary pubkey
		//
		case 'hub/get_temp_pubkey':
			var permanent_pubkey;
			var device_address;

			//	...
			permanent_pubkey = params;

			if ( ! permanent_pubkey )
			{
				return sendErrorResponse( ws, tag, "no permanent_pubkey" );
			}
			if ( permanent_pubkey.length !== _constants.PUBKEY_LENGTH )
			{
				return sendErrorResponse(ws, tag, "wrong permanent_pubkey length");
			}

			//	...
			device_address	= _object_hash.getDeviceAddress( permanent_pubkey );

			if ( device_address === m_sMyDeviceAddress )
			{
				//	to me
				//	this package signs my permanent key
				return sendResponse( ws, tag, m_objMyTempPubkeyPackage );
			}
			if ( ! _conf.bServeAsHub )
			{
				return sendErrorResponse( ws, tag, "I'm not a hub" );
			}

			_db.query
			(
				"SELECT temp_pubkey_package FROM devices WHERE device_address=?",
				[
					device_address
				],
				function( rows )
				{
					var objTempPubkey;

					if ( rows.length === 0 )
					{
						return sendErrorResponse( ws, tag, "device with this pubkey is not registered here" );
					}
					if ( ! rows[ 0 ].temp_pubkey_package )
					{
						return sendErrorResponse( ws, tag, "temp pub key not set yet" );
					}

					//	...
					objTempPubkey	= JSON.parse( rows[ 0 ].temp_pubkey_package );
					sendResponse( ws, tag, objTempPubkey );
				}
			);
			break;

		//	I'm a hub, the peer wants to update its temporary pubkey
		case 'hub/temp_pubkey':
			var objTempPubkey;
			var fnUpdate;

			if ( ! _conf.bServeAsHub )
				return sendErrorResponse( ws, tag, "I'm not a hub" );

			if ( ! ws.device_address )
				return sendErrorResponse( ws, tag, "please log in first" );

			//	...
			objTempPubkey = params;

			if ( ! objTempPubkey || ! objTempPubkey.temp_pubkey || ! objTempPubkey.pubkey || ! objTempPubkey.signature )
			{
				return sendErrorResponse( ws, tag, "no temp_pubkey params" );
			}
			if ( objTempPubkey.temp_pubkey.length !== _constants.PUBKEY_LENGTH )
			{
				return sendErrorResponse( ws, tag, "wrong temp_pubkey length" );
			}
			if ( _object_hash.getDeviceAddress( objTempPubkey.pubkey ) !== ws.device_address )
			{
				return sendErrorResponse( ws, tag, "signed by another pubkey" );
			}
			if ( ! _ecdsa_sig.verify( _object_hash.getDeviceMessageHashToSign( objTempPubkey ), objTempPubkey.signature, objTempPubkey.pubkey ) )
			{
				return sendErrorResponse(ws, tag, "wrong signature");
			}

			//	...
			fnUpdate = function( onDone )
			{
				_db.query
				(
					"UPDATE devices SET temp_pubkey_package=? WHERE device_address=?",
					[
						JSON.stringify( objTempPubkey ),
						ws.device_address
					],
					function()
					{
						if ( onDone )
						{
							onDone();
						}
					}
				);
			};

			//	...
			fnUpdate
			(
				function()
				{
					sendResponse( ws, tag, "updated" );
				}
			);
			if ( ! ws.bLoginComplete )
			{
				ws.onLoginComplete = fnUpdate;
			}
			break;

		case 'light/get_history':
			if ( _conf.bLight )
			{
				return sendErrorResponse(ws, tag, "I'm light myself, can't serve you");
			}
			if ( ws.bOutbound )
			{
				return sendErrorResponse(ws, tag, "light clients have to be inbound");
			}

			_mutex.lock
			(
				[ 'get_history_request' ],
				function( unlock )
				{
					if ( ! ws || ws.readyState !== ws.OPEN )
					{
						//	may be already gone when we receive the lock
						return process.nextTick( unlock );
					}

					_light.prepareHistory
					(
						params,
						{
							ifError : function( err )
							{
								sendErrorResponse( ws, tag, err );
								unlock();
							},
							ifOk : function( objResponse )
							{
								sendResponse( ws, tag, objResponse );

								if ( params.addresses )
								{
									_db.query
									(
										"INSERT " + _db.getIgnore()
										+ " INTO watched_light_addresses (peer, address) VALUES "
										+ params.addresses.map( function( address ){ return "(" + _db.escape( ws.peer ) + ", " + _db.escape( address ) + ")"; } ).join( ", " )
									);
								}

								if ( params.requested_joints )
								{
									_storage.sliceAndExecuteQuery
									(
										"SELECT unit FROM units WHERE main_chain_index >= ? AND unit IN(?)",
										[
											_storage.getMinRetrievableMci(),
											params.requested_joints
										],
										params.requested_joints,
										function( rows )
										{
											if ( rows.length )
											{
												_db.query
												(
													"INSERT " + _db.getIgnore()
													+ " INTO watched_light_units (peer, unit) VALUES "
													+ rows.map( function( row )
													{
														return "(" + _db.escape( ws.peer ) + ", " + _db.escape( row.unit ) + ")";
													}).join( ", " )
												);
											}
										}
									);
								}

								//	_db.query("INSERT "+_db.getIgnore()+" INTO light_peer_witnesses (peer, witness_address) VALUES "+
								//	params.witnesses.map(function(address){ return "("+_db.escape(ws.peer)+", "+_db.escape(address)+")"; }).join(", "));
								unlock();
							}
						}
					);
				}
			);
			break;

		case 'light/get_link_proofs':
			if ( _conf.bLight )
			{
				return sendErrorResponse( ws, tag, "I'm light myself, can't serve you" );
			}
			if ( ws.bOutbound )
			{
				return sendErrorResponse( ws, tag, "light clients have to be inbound" );
			}

			_mutex.lock
			(
				[ 'get_link_proofs_request' ],
				function( unlock )
				{
					if ( ! ws ||
						ws.readyState !== ws.OPEN )
					{
						//	may be already gone when we receive the lock
						return process.nextTick( unlock );
					}

					_light.prepareLinkProofs
					(
						params,
						{
							ifError : function( err )
							{
								sendErrorResponse( ws, tag, err );
								unlock();
							},
							ifOk : function( objResponse )
							{
								sendResponse( ws, tag, objResponse );
								unlock();
							}
						}
					);
				}
			);
			break;

	   case 'light/get_parents_and_last_ball_and_witness_list_unit' :
			if ( _conf.bLight )
			{
				return sendErrorResponse( ws, tag, "I'm light myself, can't serve you" );
			}
			if ( ws.bOutbound )
			{
				return sendErrorResponse( ws, tag, "light clients have to be inbound" );
			}
			if ( ! params )
			{
				return sendErrorResponse( ws, tag, "no params in get_parents_and_last_ball_and_witness_list_unit" );
			}

			//	...
			_light.prepareParentsAndLastBallAndWitnessListUnit
			(
				params.witnesses,
				{
					ifError : function( err )
					{
						sendErrorResponse( ws, tag, err );
					},
					ifOk : function( objResponse )
					{
						sendResponse( ws, tag, objResponse );
					}
				}
			);
			break;

	   case 'light/get_attestation':
			var order;
			var join;

			if ( _conf.bLight )
			{
				return sendErrorResponse( ws, tag, "I'm light myself, can't serve you" );
			}
			if ( ws.bOutbound )
			{
				return sendErrorResponse( ws, tag, "light clients have to be inbound" );
			}
			if ( ! params )
			{
				return sendErrorResponse( ws, tag, "no params in light/get_attestation" );
			}
			if ( ! params.attestor_address || ! params.field || ! params.value )
			{
				return sendErrorResponse( ws, tag, "missing params in light/get_attestation" );
			}

			//	...
			order	= ( _conf.storage === 'sqlite' ) ? 'rowid' : 'creation_date';
			join	= ( _conf.storage === 'sqlite' ) ? '' : 'JOIN units USING(unit)';

			_db.query
			(
				"SELECT unit FROM attested_fields " + join + " WHERE attestor_address=? AND field=? AND value=? ORDER BY " + order + " DESC LIMIT 1",
				[
					params.attestor_address,
					params.field,
					params.value
				],
				function( rows )
				{
					var attestation_unit;

					//	...
					attestation_unit	= ( rows.length > 0 ) ? rows[ 0 ].unit : "";
					sendResponse( ws, tag, attestation_unit );
				}
			);
			break;

		//	I'm a hub, the peer wants to enable push notifications
		case 'hub/enable_notification':
			if ( ws.device_address )
			{
				_event_bus.emit( "enableNotification", ws.device_address, params );
			}

			sendResponse( ws, tag, 'ok' );
			break;

		//	I'm a hub, the peer wants to disable push notifications
		case 'hub/disable_notification':
			if ( ws.device_address )
			{
				_event_bus.emit( "disableNotification", ws.device_address, params );
			}

			sendResponse( ws, tag, 'ok' );
			break;

		case 'hub/get_bots':
			_db.query
			(
				"SELECT id, name, pairing_code, description FROM bots ORDER BY rank DESC, id",
				[],
				function( rows )
				{
					sendResponse( ws, tag, rows );
				}
			);
			break;

		case 'hub/get_asset_metadata':
			var asset;

			//	...
			asset	= params;
			if ( ! _validation_utils.isStringOfLength( asset, _constants.HASH_LENGTH ) )
			{
				return sendErrorResponse(ws, tag, "bad asset: "+asset);
			}

			_db.query
			(
				"SELECT metadata_unit, registry_address, suffix FROM asset_metadata WHERE asset=?",
				[
					asset
				],
				function( rows )
				{
					if ( rows.length === 0 )
					{
						return sendErrorResponse( ws, tag, "no metadata" );
					}

					sendResponse( ws, tag, rows[ 0 ] );
				}
			);
			break;
	}
}

function onWebSocketMessage( message )
{
	var ws;
	var arrMessage;
	var sMessageType;
	var oContent;

	//	...
	ws = this;

	if ( ws.readyState !== ws.OPEN )
	{
		return;
	}

	//	...
	//_log.consoleLog( 'RECEIVED ' + ( message.length > 1000 ? message.substr( 0, 1000 ) + '... (' + message.length + ' chars)' : message ) + ' from ' + ws.peer );
	_logex.push( 'RECEIVED ' + ( message.length > 1000 ? message.substr( 0, 1000 ) + '... (' + message.length + ' chars)' : message ) + ' from ' + ws.peer );

	//	...
	ws.last_ts	= Date.now();

	try
	{
		arrMessage	= JSON.parse( message );
	}
	catch( e )
	{
		return _log.consoleLog( 'failed to json.parse message ' + message );
	}

	//	...
	sMessageType	= arrMessage[ 0 ];
	oContent	= arrMessage[ 1 ];

	switch ( sMessageType )
	{
		case 'justsaying':
			return handleJustsaying( ws, oContent.subject, oContent.body );

		case 'request':
			return handleRequest( ws, oContent.tag, oContent.command, oContent.params );

		case 'response':
			return handleResponse( ws, oContent.tag, oContent.response );

		default: 
			_log.consoleLog( "unknown type: " + sMessageType );
			//	throw Error("unknown type: " + sMessageType);
	}
}


/**
 *	start server
 */
function startAcceptingConnections()
{
	//
	//	delete all ...
	//
	_db.query( "DELETE FROM watched_light_addresses" );
	_db.query( "DELETE FROM watched_light_units" );

	//
	//	create a new web socket server
	//
	//	npm ws
	//	https://github.com/websockets/ws
	//
	//	_db.query("DELETE FROM light_peer_witnesses");
	//	listen for new connections
	//
	m_oWss	= new WebSocketServer
	(
		{
			port	: _conf.port
		}
	);

	//
	//	Event 'connection'
	//		Emitted when the handshake is complete.
	//
	//		- socket	{ WebSocket }
	//		- request	{ http.IncomingMessage }
	//
	//		request is the http GET request sent by the client.
	// 		Useful for parsing authority headers, cookie headers, and other information.
	//
	m_oWss.on
	(
		'connection',
		function( ws )
		{
			var sRemoteAddress;
			var bStatsCheckUnderWay;

			//	...
			sRemoteAddress = ws.upgradeReq.connection.remoteAddress;
			if ( ! sRemoteAddress )
			{
				_log.consoleLog( "no ip/sRemoteAddress in accepted connection" );
				ws.terminate();
				return;
			}
			if ( ws.upgradeReq.headers[ 'x-real-ip' ] && ( sRemoteAddress === '127.0.0.1' || sRemoteAddress.match( /^192\.168\./ ) ) )
			{
				//
				//	TODO
				//	check for resources IP addresses
				//

				//	we are behind a proxy
				sRemoteAddress = ws.upgradeReq.headers[ 'x-real-ip' ];
			}

			//	...
			ws.peer				= sRemoteAddress + ":" + ws.upgradeReq.connection.remotePort;
			ws.host				= sRemoteAddress;
			ws.assocPendingRequests		= {};
			ws.assocInPreparingResponse	= {};
			ws.bInbound			= true;
			ws.last_ts			= Date.now();

			_log.consoleLog( 'got connection from ' + ws.peer + ", host " + ws.host );

			if ( m_oWss.arrClients.length >= _conf.MAX_INBOUND_CONNECTIONS )
			{
				_log.consoleLog( "inbound connections maxed out, rejecting new client " + sRemoteAddress );

				//	1001 doesn't work in cordova
				ws.close( 1000, "inbound connections maxed out" );
				return;
			}

			//	...
			bStatsCheckUnderWay	= true;

			//
			//	calculate the counts of elements in status invalid and new_good
			//	from table [peer_events] by peer_host for a hour ago.
			//
			_db.query
			(
				"SELECT \n\
					SUM( CASE WHEN event='invalid' THEN 1 ELSE 0 END ) AS count_invalid, \n\
					SUM( CASE WHEN event='new_good' THEN 1 ELSE 0 END ) AS count_new_good \n\
					FROM peer_events WHERE peer_host = ? AND event_date > " + _db.addTime( "-1 HOUR" ),
				[
					//	remote host/sRemoteAddress connected by this ws
					ws.host
				],
				function( rows )
				{
					var oStats;

					//	...
					bStatsCheckUnderWay	= false;

					//	...
					oStats	= rows[ 0 ];
					if ( oStats.count_invalid )
					{
						//
						//	CONNECTION WAS REJECTED
						//	this peer have invalid events before
						//
						_log.consoleLog( "# rejecting new client " + ws.host + " because of bad stats" );
						return ws.terminate();
					}

					//
					//	WELCOME THE NEW PEER WITH THE LIST OF FREE JOINTS
					//
					//	if (!m_bCatchingUp)
					//		sendFreeJoints(ws);
					//
					//	*
					//	so, we response the version of this hub/witness
					//
					sendVersion( ws );

					//	I'm a hub, send challenge
					if ( _conf.bServeAsHub )
					{
						ws.challenge	= _crypto.randomBytes( 30 ).toString( "base64" );

						//
						//	the the new peer, I am a hub and I have ability to exchange data
						//
						sendJustSaying( ws, 'hub/challenge', ws.challenge );
					}
					if ( ! _conf.bLight )
					{
						//
						//	subscribe data from others
						//
						subscribe( ws );
					}

					//
					//	emit a event say there was a client connected
					//
					_event_bus.emit( 'connected', ws );
				}
			);

			//
			//	receive message
			//
			ws.on
			(
				'message',
				function( message )
				{
					//	might come earlier than stats check completes
					function tryHandleMessage()
					{
						if ( bStatsCheckUnderWay )
						{
							setTimeout
							(
								tryHandleMessage,
								100
							);
						}
						else
						{
							onWebSocketMessage.call( ws, message );
						}
					}

					//	...
					tryHandleMessage();
				}
			);

			//
			//	on close
			//
			ws.on
			(
				'close',
				function()
				{
					_db.query( "DELETE FROM watched_light_addresses WHERE peer=?", [ ws.peer ] );
					_db.query( "DELETE FROM watched_light_units WHERE peer=?", [ ws.peer ] );
					//_db.query( "DELETE FROM light_peer_witnesses WHERE peer=?", [ ws.peer ] );
					_log.consoleLog( "client " + ws.peer + " disconnected" );

					cancelRequestsOnClosedConnection( ws );
				}
			);

			//
			//	on error
			//
			ws.on
			(
				'error',
				function( e )
				{
					_log.consoleLog( "error on client " + ws.peer + ": " + e );

					//	close
					ws.close( 1000, "received error" );
				}
			);

			//	...
			addPeerHost( ws.host );
		}
	);
	_log.consoleLog( 'WSS running at port ' + _conf.port );
}


/**
 *	start relay(hub)
 */
function startRelay()
{
	if ( process.browser || ! _conf.port )
	{
		//	no listener on mobile
		m_oWss = { arrClients : [] };
	}
	else
	{
		//
		//	user configuration a port, so we will start a listening socket service
		//	*
		//	prepare to accepting connections
		//
		startAcceptingConnections();
	}

	//	...
	checkCatchupLeftovers();


	//
	//	the default value of _conf.bWantNewPeers is true
	//
	if ( _conf.bWantNewPeers )
	{
		_log.consoleLog( "network::startRelay, _conf.bWantNewPeers = true" );

		//
		//	add outbound connections
		//
		//	retry lost and failed connections
		//	every 1 minute
		//
		addOutboundPeers();
		setInterval
		(
			addOutboundPeers,
			60 * 1000
		);

		//
		//	...
		//
		setTimeout
		(
			checkIfHaveEnoughOutboundPeersAndAdd,
			30 * 1000
		);

		//
		//	purge dead peers
		//	every half hour
		//
		setInterval
		(
			purgeDeadPeers,
			30 * 60 * 1000
		);
	}

	//
	//	purge peer_events
	//	removing those older than 3 days ago.
	//	every 6 hours
	//
	setInterval
	(
		purgePeerEvents,
		6 * 60 * 60 * 1000
	);

	//
	//	request needed joints that were not received during the previous session
	//	every 8 seconds
	//
	rerequestLostJoints();
	setInterval
	(
		rerequestLostJoints,
		8 * 1000
	);

	//
	//	purge junk unhandled joints
	//	every half an hour
	//
	setInterval
	(
		purgeJunkUnhandledJoints,
		30 * 60 * 1000
	);

	//
	//	purge uncovered nonserial joints under lock
	//	every 1 minute
	//
	setInterval
	(
		_joint_storage.purgeUncoveredNonserialJointsUnderLock,
		60 * 1000
	);

	//
	//	find and handle joints that are ready
	//	every 5 seconds
	//
	setInterval
	(
		findAndHandleJointsThatAreReady,
		5 * 1000
	);
}

/**
 *	start light client
 */
function startLightClient()
{
	m_oWss	= { arrClients: [] };

	//
	//	re-request lost joints of private payment
	//
	rerequestLostJointsOfPrivatePayments();
	setInterval
	(
		rerequestLostJointsOfPrivatePayments,
		5 * 1000
	);

	//
	//	handle saved private payment
	//	every 5 seconds
	//
	setInterval
	(
		handleSavedPrivatePayments,
		5 * 1000
	);

	//
	//	request unfinished post units of saved private elements
	//	every 12 seconds
	//
	setInterval
	(
		requestUnfinishedPastUnitsOfSavedPrivateElements,
		12 * 1000
	);
}


/**
 *	network start
 */
function start()
{
	_log.consoleLog( "############################################################" );
	_log.consoleLog( "starting network" );
	_log.consoleLog( "############################################################" );

	//	...
	_conf.bLight ? startLightClient() : startRelay();

	//	...
	setInterval
	(
		printConnectionStatus,
		6 * 1000
	);

	//
	//	if we have exactly same intervals on two clints,
	//	they might send heartbeats to each other at the same time
	//
	setInterval
	(
		heartbeat,
		3 * 1000 + getRandomInt( 0, 1000 )
	);
}

function closeAllWsConnections()
{
	m_arrOutboundPeers.forEach( function( ws )
	{
		ws.close( 1000, 'Re-connect' );
	});
}

function isConnected()
{
	return ( m_arrOutboundPeers.length + m_oWss.arrClients.length );
}

function isCatchingUp()
{
	return m_bCatchingUp;
}








/**
 *	Adding support for browser
 */
if ( process.browser )
{
	//	browser
	_log.consoleLog( "defining .on() on ws" );

	WebSocket.prototype.on = function( event, callback )
	{
		var self;

		//	...
		self = this;

		if ( event === 'message' )
		{
			this[ 'on' + event ] = function( event )
			{
				callback.call( self, event.data );
			};
			return;
		}
		if ( event !== 'open' )
		{
			this[ 'on' + event ] = callback;
			return;
		}

		//	allow several handlers for 'open' event
		if ( ! this[ 'open_handlers' ] )
		{
			this[ 'open_handlers' ] = [];
		}

		this[ 'open_handlers' ].push( callback );
		this[ 'on' + event ] = function()
		{
			self[ 'open_handlers' ].forEach
			(
				function( cb )
				{
					cb();
				}
			);
		};
	};

	//	...
	WebSocket.prototype.once		= WebSocket.prototype.on;
	WebSocket.prototype.setMaxListeners	= function(){};
}





/**
 *	...
 */
if ( ! _conf.bLight )
{
	setInterval
	(
		function()
		{
			flushEvents( true );
		},
		1000 * 60
	);
}



/**
 *	mci
 */
_event_bus.on
(
	'mci_became_stable',
	notifyWatchersAboutStableJoints
);


/**
 *	start
 */
start();






/**
 *	exports
 */
exports.start						= start;
exports.postJointToLightVendor				= postJointToLightVendor;
exports.broadcastJoint					= broadcastJoint;
exports.sendPrivatePayment				= sendPrivatePayment;

exports.sendJustSaying					= sendJustSaying;
exports.sendAllInboundJustSaying			= sendAllInboundJustSaying;
exports.sendError					= sendError;
exports.sendRequest					= sendRequest;
exports.findOutboundPeerOrConnect			= findOutboundPeerOrConnect;
exports.handleOnlineJoint				= handleOnlineJoint;

exports.handleOnlinePrivatePayment			= handleOnlinePrivatePayment;
exports.requestUnfinishedPastUnitsOfPrivateChains	= requestUnfinishedPastUnitsOfPrivateChains;
exports.requestProofsOfJointsIfNewOrUnstable		= requestProofsOfJointsIfNewOrUnstable;

exports.requestFromLightVendor				= requestFromLightVendor;

exports.addPeer						= addPeer;

exports.initWitnessesIfNecessary			= initWitnessesIfNecessary;

exports.setMyDeviceProps				= setMyDeviceProps;

exports.setWatchedAddresses				= setWatchedAddresses;
exports.addWatchedAddress				= addWatchedAddress;
exports.addLightWatchedAddress				= addLightWatchedAddress;

exports.closeAllWsConnections				= closeAllWsConnections;
exports.isConnected					= isConnected;
exports.isCatchingUp					= isCatchingUp;
exports.requestHistoryFor				= requestHistoryFor;
exports.exchangeRates					= m_exchangeRates;
