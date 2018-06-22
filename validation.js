/*jslint node: true */
"use strict";

var _					= require( 'lodash' );
var _async				= require( 'async' );
var callee				= require( './callee' );
var _log				= require( './log.js' );
var _storage				= require( './storage.js' );
var _object_hash			= require( './object_hash.js' );
var _object_length			= require( './object_length.js' );
var _db					= require( './db.js' );
var _mutex				= require( './mutex.js' );
var _constants				= require( './constants.js' );
var _definition				= require( './definition.js' );
var _conf				= require( './conf.js' );
var _profiler_ex			= require( './profilerex.js' );

var _validation_utils			= require( './validation_utils.js' );
var _validation_validate_parents	= require( './validation_validate_parents.js' );
var _validation_validate_witnesses	= require( './validation_validate_witnesses.js' );
var _validation_validate_authors	= require( './validation_validate_authors.js' );
var _validation_validate_payment	= require( './validation_validate_payment.js' );
var _validation_validate_messages	= require( './validation_validate_messages.js' );




/**
 *	@public
 *
 *	@param objJoint
 *	@returns {boolean}
 */
function hasValidHashes( objJoint )
{
	return ( _object_hash.getUnitHash( objJoint.unit ) === objJoint.unit.unit );
}



/**
 *	@public
 *	validate
 *
 *	@param objJoint
 *	@param callbacks
 *	@returns {*|void}
 */
function validate( objJoint, callbacks )
{
	var objUnit;
	var arrAuthorAddresses;
	var objValidationState;

	//	...
	objUnit	= objJoint.unit;

	if ( typeof objUnit !== "object" || objUnit === null )
	{
		throw Error( "no unit object" );
	}
	if ( ! objUnit.unit )
	{
		throw Error( "no unit" );
	}

	//	...
	_log.consoleLog( "validating joint identified by unit " + objJoint.unit.unit );

	if ( ! _validation_utils.isStringOfLength( objUnit.unit, _constants.HASH_LENGTH ) )
	{
		return callee.callee( callbacks, 'ifJointError', 'wrong unit length' );
	}

	try
	{
		//
		//	UnitError is linked to objUnit.unit,
		//	so we need to ensure objUnit.unit is true before we throw any UnitErrors
		//
		if ( _object_hash.getUnitHash( objUnit ) !== objUnit.unit )
		{
			return callee.callee( callbacks, 'ifJointError', "wrong unit hash: " + _object_hash.getUnitHash( objUnit ) + " != " + objUnit.unit );
		}
	}
	catch ( e )
	{
		return callee.callee( callbacks, 'ifJointError', "failed to calc unit hash: " + e );
	}


	if ( objJoint.unsigned )
	{
		if ( _validation_utils.hasFieldsExcept( objJoint, [ "unit", "unsigned" ] ) )
		{
			return callee.callee( callbacks, 'ifJointError', "unknown fields in unsigned unit-joint" );
		}
	}
	else if ( "ball" in objJoint )
	{
		if ( ! _validation_utils.isStringOfLength( objJoint.ball, _constants.HASH_LENGTH ) )
		{
			return callee.callee( callbacks, 'ifJointError', "wrong ball length" );
		}
		if ( _validation_utils.hasFieldsExcept( objJoint, [ "unit", "ball", "skiplist_units" ] ) )
		{
			return callee.callee( callbacks, 'ifJointError', "unknown fields in ball-joint" );
		}
		if ( "skiplist_units" in objJoint )
		{
			if ( ! _validation_utils.isNonemptyArray( objJoint.skiplist_units ) )
			{
				return callee.callee( callbacks, 'ifJointError', "missing or empty skiplist array" );
			}
			//if (objUnit.unit.charAt(0) !== "0")
			//    return callee.callee( callbacks, 'ifJointError', "found skiplist while unit doesn't start with 0");
		}
	}
	else
	{
		if ( _validation_utils.hasFieldsExcept( objJoint, [ "unit" ] ) )
		{
			return callee.callee( callbacks, 'ifJointError', "unknown fields in unit-joint" );
		}
	}

	//	...
	if ( "content_hash" in objUnit )
	{
		//	nonserial and stripped off content
		if ( ! _validation_utils.isStringOfLength( objUnit.content_hash, _constants.HASH_LENGTH ) )
		{
			return callee.callee( callbacks, 'ifUnitError', "wrong content_hash length" );
		}
		if ( _validation_utils.hasFieldsExcept( objUnit,
			[ "unit", "version", "alt", "timestamp", "authors", "witness_list_unit", "witnesses", "content_hash", "parent_units", "last_ball", "last_ball_unit" ] ) )
		{
			return callee.callee( callbacks, 'ifUnitError', "unknown fields in nonserial unit" );
		}
		if ( ! objJoint.ball )
		{
			return callee.callee( callbacks, 'ifJointError', "content_hash allowed only in finished ball" );
		}
	}
	else
	{
		//	serial
		if ( _validation_utils.hasFieldsExcept( objUnit,
			[ "unit", "version", "alt", "timestamp", "authors", "messages", "witness_list_unit", "witnesses", "earned_headers_commission_recipients", "last_ball", "last_ball_unit", "parent_units", "headers_commission", "payload_commission" ] ) )
		{
			return callee.callee( callbacks, 'ifUnitError', "unknown fields in unit" );
		}
		if ( typeof objUnit.headers_commission !== "number" )
		{
			return callee.callee( callbacks, 'ifJointError', "no headers_commission" );
		}
		if ( typeof objUnit.payload_commission !== "number" )
		{
			return callee.callee( callbacks, 'ifJointError', "no payload_commission" );
		}

		if ( ! _validation_utils.isNonemptyArray( objUnit.messages ) )
		{
			return callee.callee( callbacks, 'ifUnitError', "missing or empty messages array" );
		}
		if ( objUnit.messages.length > _constants.MAX_MESSAGES_PER_UNIT )
		{
			return callee.callee( callbacks, 'ifUnitError', "too many messages" );
		}

		if ( _object_length.getHeadersSize( objUnit ) !== objUnit.headers_commission )
		{
			return callee.callee( callbacks, 'ifJointError', "wrong headers commission, expected " + _object_length.getHeadersSize( objUnit ) );
		}
		if ( _object_length.getTotalPayloadSize( objUnit ) !== objUnit.payload_commission )
		{
			return callee.callee
			(
				callbacks,
				'ifJointError',
				"wrong payload commission, unit " + objUnit.unit + ", calculated " + _object_length.getTotalPayloadSize( objUnit ) + ", expected " + objUnit.payload_commission
			);
		}
	}

	if ( ! _validation_utils.isNonemptyArray( objUnit.authors ) )
	{
		return callee.callee( callbacks, 'ifUnitError', "missing or empty authors array" );
	}
	if ( objUnit.version !== _constants.version )
	{
		return callee.callee( callbacks, 'ifUnitError', "wrong version" );
	}
	if ( objUnit.alt !== _constants.alt )
	{
		return callee.callee( callbacks, 'ifUnitError', "wrong alt" );
	}

	if ( ! _storage.isGenesisUnit( objUnit.unit ) )
	{
		if ( ! _validation_utils.isNonemptyArray( objUnit.parent_units ) )
		{
			return callee.callee( callbacks, 'ifUnitError', "missing or empty parent units array" );
		}
		if ( ! _validation_utils.isStringOfLength( objUnit.last_ball, _constants.HASH_LENGTH ) )
		{
			return callee.callee( callbacks, 'ifUnitError', "wrong length of last ball" );
		}
		if ( ! _validation_utils.isStringOfLength( objUnit.last_ball_unit, _constants.HASH_LENGTH ) )
		{
			return callee.callee( callbacks, 'ifUnitError', "wrong length of last ball unit" );
		}
	}

	if ( "witness_list_unit" in objUnit &&
		"witnesses" in objUnit )
	{
		return callee.callee( callbacks, 'ifUnitError', "ambiguous witnesses" );
	}

	if ( _conf.bLight )
	{
		if ( ! _validation_utils.isPositiveInteger( objUnit.timestamp ) && ! objJoint.unsigned )
		{
			return callee.callee( callbacks, 'ifJointError', "bad timestamp" );
		}
		if ( objJoint.ball )
		{
			return callee.callee( callbacks, 'ifJointError', "I'm light, can't accept stable unit " + objUnit.unit + " without proof" );
		}

		return objJoint.unsigned 
			? callee.callee( callbacks, 'ifOkUnsigned', true )
			: callee.callee
			(
				callbacks,
				'ifOk',
				{
					sequence		: 'good',
					arrDoubleSpendInputs	: [],
					arrAdditionalQueries	: []
				},
				function(){}
			);
	}
	else
	{
		if ( "timestamp" in objUnit && ! _validation_utils.isPositiveInteger( objUnit.timestamp ) )
		{
			return callee.callee( callbacks, 'ifJointError', "bad timestamp" );
		}
	}


	//
	//	...
	//
	arrAuthorAddresses = objUnit.authors
		? objUnit.authors.map( function( author )
			{
				return author.address;
			} )
		: [];

	objValidationState =
		{
			arrAdditionalQueries	: [],
			arrDoubleSpendInputs	: [],
			arrInputKeys		: []
		};

	if ( objJoint.unsigned )
	{
		objValidationState.bUnsigned	= true;
	}

	//	...
	_profiler_ex.begin( "#validate" );

	//	...
	_mutex.lock
	(
		arrAuthorAddresses,
		function( unlock )
		{
			var conn = null;

			//	...
			_async.series
			(
				[
					function( cb )
					{
						//	PPP
						_profiler_ex.begin( 'validation-takeConnectionFromPool' );

						_db.takeConnectionFromPool
						(
							function( new_conn )
							{
								_profiler_ex.end( 'validation-takeConnectionFromPool' );

								//	...
								_profiler_ex.begin( 'validation-BEGIN' );

								//	...
								conn = new_conn;
								conn.query
								(
									"BEGIN",
									function()
									{
										_profiler_ex.end( 'validation-BEGIN' );
										cb();
									}
								);
							}
						);
					},
					function( cb )
					{
						_profiler_ex.begin( 'validation-checkDuplicate' );

						//	...
						_checkDuplicate
						(
							conn,
							objUnit.unit,
							function ()
							{
								_profiler_ex.end( 'validation-checkDuplicate' );

								//	...
								cb.apply( this, arguments );
							}
						);
					},
					function( cb )
					{
						_profiler_ex.begin( 'validation-validateHeadersCommissionRecipients' );

						objUnit.content_hash
							? function ()
							{
								_profiler_ex.end( 'validation-validateHeadersCommissionRecipients' );
								cb();
							}()
							: _validateHeadersCommissionRecipients
							(
								objUnit,
								function ()
								{
									_profiler_ex.end( 'validation-validateHeadersCommissionRecipients' );
									cb.apply( this, arguments );
								}
							);
					},
					function( cb )
					{
						_profiler_ex.begin( 'validation-validateHashTree' );

						! objUnit.parent_units
							? function ()
							{
								_profiler_ex.end( 'validation-validateHashTree' );
								cb();
							}()
							: _validateHashTree
							(
								conn,
								objJoint,
								objValidationState,
								function ()
								{
									_profiler_ex.end( 'validation-validateHashTree' );
									cb.apply( this, arguments );
								}
							);
					},
					function( cb )
					{
						_profiler_ex.begin( 'validation-validateParents' );

						! objUnit.parent_units
							? function ()
							{
								_profiler_ex.end( 'validation-validateParents' );
								cb();
							}()
							: _validateParents
							(
								conn,
								objJoint,
								objValidationState,
								function ()
								{
									_profiler_ex.end( 'validation-validateParents' );
									cb.apply( this, arguments );
								}
							);
					},
					function( cb )
					{
						_profiler_ex.begin( 'validation-validateSkiplist' );

						! objJoint.skiplist_units
							? function ()
							{
								_profiler_ex.end( 'validation-validateSkiplist' );
								cb();
							}()
							: _validateSkipList
							(
								conn,
								objJoint.skiplist_units,
								function ()
								{
									_profiler_ex.end( 'validation-validateSkiplist' );
									cb.apply( this, arguments );
								}
							);
					},
					function( cb )
					{
						_profiler_ex.begin( 'validation-validateWitnesses' );

						//	...
						_validateWitnesses
						(
							conn,
							objUnit,
							objValidationState,
							function ()
							{
								_profiler_ex.end( 'validation-validateWitnesses' );
								cb.apply( this, arguments );
							}
						);
					},
					function( cb )
					{
						_profiler_ex.begin( 'validation-validateAuthors' );

						//	...
						_validateAuthors
						(
							conn,
							objUnit.authors,
							objUnit,
							objValidationState,
							function ()
							{
								_profiler_ex.end( 'validation-validateAuthors' );
								cb.apply( this, arguments );
							}
						);
					},
					function( cb )
					{
						_profiler_ex.begin( 'validation-validateMessages' );

						//	...
						objUnit.content_hash
							? function ()
							{
								_profiler_ex.end( 'validation-validateMessages' );
								cb();
							}()
							: _validateMessages
							(
								conn,
								objUnit.messages,
								objUnit,
								objValidationState,
								function ()
								{
									_profiler_ex.end( 'validation-validateMessages' );
									cb.apply( this, arguments );
								}
							);
					}
				],
				function( err )
				{
					if ( err )
					{
						_profiler_ex.begin( 'validation-ROLLBACK' );

						//	...
						conn.query
						(
							"ROLLBACK",
							function()
							{
								_profiler_ex.end( "#validate" );

								//	...
								conn.release();
								unlock();

								//	PPP
								_profiler_ex.end( 'validation-ROLLBACK' );

								//	...
								if ( typeof err === "object" )
								{
									if ( err.error_code === "unresolved_dependency" )
									{
										callee.callee( callbacks, 'ifNeedParentUnits', err.arrMissingUnits );
									}
									else if ( err.error_code === "need_hash_tree" )
									{
										//	need to download hash tree to catch up
										callee.callee( callbacks, 'ifNeedHashTree' );
									}
									else if ( err.error_code === "invalid_joint" )
									{
										//	ball found in hash tree but with another unit
										callee.callee( callbacks, 'ifJointError', err.message );
									}
									else if ( err.error_code === "transient" )
									{
										callee.callee( callbacks, 'ifTransientError', err.message );
									}
									else
									{
										throw Error( "unknown error code" );
									}
								}
								else
								{
									callee.callee( callbacks, 'ifUnitError', err );
								}
							}
						);
					}
					else
					{
						_profiler_ex.begin( 'validation-COMMIT' );

						//	...
						conn.query
						(
							"COMMIT", function()
							{
								_profiler_ex.end( "#validate" );

								//	...
								conn.release();

								//	...
								_profiler_ex.end( 'validation-COMMIT' );

								if ( objJoint.unsigned )
								{
									unlock();
									callee.callee( callbacks, 'ifOkUnsigned', objValidationState.sequence === 'good' );
								}
								else
								{
									callee.callee( callbacks, 'ifOk', objValidationState, unlock );
								}
							}
						);
					}
				}
			);	//	_async.series
		}
	);
}


/**
 *	@public
 *
 *	used for both public and private payments
 */
function validatePayment( conn, payload, message_index, objUnit, objValidationState, callback )
{
	return _validation_validate_payment.validatePayment
	(
		conn,
		payload,
		message_index,
		objUnit,
		objValidationState,
		callback
	);
}



/**
 *	@public
 *
 *	@param conn
 *	@param unit
 *	@param message_index
 *	@param payload
 *	@param onError
 *	@param onDone
 */
function initPrivatePaymentValidationState( conn, unit, message_index, payload, onError, onDone )
{
	//	...
	conn.query
	(
		"SELECT payload_hash, app, units.sequence, units.is_stable, lb_units.main_chain_index AS last_ball_mci \n\
		FROM messages JOIN units USING(unit) \n\
		LEFT JOIN units AS lb_units ON units.last_ball_unit=lb_units.unit \n\
		WHERE messages.unit=? AND message_index=?",
		[
			unit,
			message_index
		],
		function( rows )
		{
			var row;
			var bStable;
			var objValidationState;
			var objPartialUnit;

			if ( rows.length > 1 )
			{
				throw Error( "more than 1 message by index" );
			}
			if ( rows.length === 0 )
			{
				return onError( "message not found" );
			}

			//	...
			row	= rows[ 0 ];

			if ( row.sequence !== "good" && row.is_stable === 1 )
			{
				return onError( "unit is final nonserial" );
			}

			//	it's ok if the unit is not stable yet
			bStable	= ( row.is_stable === 1 );
			if ( row.app !== "payment" )
			{
				return onError( "invalid app" );
			}
			if ( _object_hash.getBase64Hash( payload ) !== row.payload_hash )
			{
				return onError( "payload hash does not match" );
			}

			//	...
			objValidationState =
				{
					last_ball_mci		: row.last_ball_mci,
					arrDoubleSpendInputs	: [],
					arrInputKeys		: [],
					bPrivate		: true
				};
			objPartialUnit =
				{
					unit	: unit
				};

			//	...
			_storage.readUnitAuthors
			(
				conn,
				unit,
				function( arrAuthors )
				{
					//	array of objects {address: address}
					objPartialUnit.authors = arrAuthors.map
					(
						function( address )
						{
							return { address: address };
						}
					);

					//	we need parent_units in checkForDoubleSpends in case it is a doublespend
					conn.query
					(
						"SELECT parent_unit FROM parenthoods WHERE child_unit=? ORDER BY parent_unit",
						[
							unit
						],
						function( prows )
						{
							objPartialUnit.parent_units = prows.map
							(
								function( prow )
								{
									return prow.parent_unit;
								}
							);

							//	...
							onDone( bStable, objPartialUnit, objValidationState );
						}
					);
				}
			);
		}
	);
}


/**
 *	@public
 *
 *	@param objAuthor
 *	@param objUnit
 *	@param arrAddressDefinition
 *	@param callback
 */
function validateAuthorSignaturesWithoutReferences( objAuthor, objUnit, arrAddressDefinition, callback )
{
	var objValidationState;

	//	...
	objValidationState =
		{
			unit_hash_to_sign	: _object_hash.getUnitHashToSign( objUnit ),
			last_ball_mci		: -1,
			bNoReferences		: true
		};
	_definition.validateAuthentifiers
	(
		null,
		objAuthor.address,
		null,
		arrAddressDefinition,
		objUnit,
		objValidationState,
		objAuthor.authentifiers,
		function( err, res )
		{
			if ( err )
			{
				//	error in address definition
				return callback( err );
			}
			if ( ! res )
			{
				//	wrong signature or the like
				return callback( "authentifier verification failed" );
			}

			//	...
			callback();
		}
	);
}


/**
 *	@public
 *
 *	@param objSignedMessage
 *	@param handleResult
 *	@returns {*}
 */
function validateSignedMessage( objSignedMessage, handleResult )
{
	var objAuthor;
	var arrAddressDefinition;
	var objUnit;
	var objValidationState;

	if ( typeof objSignedMessage !== 'object' )
	{
		return handleResult( "not an object" );
	}
	if ( _validation_utils.hasFieldsExcept( objSignedMessage, [ "signed_message", "authors" ] ) )
	{
		return handleResult( "unknown fields" );
	}
	if ( typeof objSignedMessage.signed_message !== 'string' )
	{
		return handleResult( "signed message not a string" );
	}
	if ( ! Array.isArray( objSignedMessage.authors ) )
	{
		return handleResult( "authors not an array" );
	}
	if ( ! _validation_utils.isArrayOfLength( objSignedMessage.authors, 1 ) )
	{
		return handleResult( "authors not an array of len 1" );
	}

	//	...
	objAuthor = objSignedMessage.authors[ 0 ];
	if ( ! objAuthor )
	{
		return handleResult( "no authors[0]" );
	}
	if ( ! _validation_utils.isValidAddress( objAuthor.address ) )
	{
		return handleResult( "not valid address" );
	}
	if ( typeof objAuthor.authentifiers !== 'object' )
	{
		return handleResult( "not valid authentifiers" );
	}

	//	...
	arrAddressDefinition	= objAuthor.definition;
	if ( _object_hash.getChash160( arrAddressDefinition ) !== objAuthor.address )
	{
		return handleResult( "wrong definition: " + _object_hash.getChash160( arrAddressDefinition ) + "!==" + objAuthor.address );
	}

	//	...
	objUnit			= _.clone( objSignedMessage );
	objUnit.messages	= [];		//	some ops need it
	objValidationState	=
		{
			unit_hash_to_sign	: _object_hash.getUnitHashToSign( objSignedMessage ),
			last_ball_mci		: -1,
			bNoReferences		: true
		};

	//	passing _db as null
	_definition.validateAuthentifiers
	(
		null,
		objAuthor.address,
		null,
		arrAddressDefinition,
		objUnit,
		objValidationState,
		objAuthor.authentifiers,
		function( err, res )
		{
			if ( err )
			{
				//	error in address definition
				return handleResult( err );
			}
			if ( ! res )
			{
				//	wrong signature or the like
				return handleResult( "authentifier verification failed" );
			}

			//	...
			handleResult();
		}
	);
}


/**
 *	@public
 *
 * 	inconsistent for multisig addresses
 *	@param objSignedMessage
 *	@returns {*}
 */
function validateSignedMessageSync( objSignedMessage )
{
	var err		= null;
	var bCalledBack = false;

	//	...
	validateSignedMessage
	(
		objSignedMessage,
		function( _err )
		{
			err = _err;
			bCalledBack = true;
		}
	);

	if ( ! bCalledBack )
	{
		throw Error( "validateSignedMessage is not sync" );
	}

	//	...
	return err;
}









//	--------------------------------------------------------------------------------
//	Private
//	--------------------------------------------------------------------------------


function _checkDuplicate( conn, unit, cb )
{
	conn.query
	(
		"SELECT 1 FROM units WHERE unit=?",
		[
			unit
		],
		function( rows )
		{
			if ( rows.length === 0 )
			{
				return cb();
			}

			cb( "unit " + unit + " already exists" );
		}
	);
}

function _validateHashTree( conn, objJoint, objValidationState, callback )
{
	var objUnit;

	if ( ! objJoint.ball )
	{
		return callback();
	}

	//	...
	objUnit = objJoint.unit;

	//	...
	conn.query
	(
		"SELECT unit FROM hash_tree_balls WHERE ball=?",
		[
			objJoint.ball
		],
		function( rows )
		{
			if ( rows.length === 0 )
			{
				return callback
				(
					{
						error_code	: "need_hash_tree",
						message		: "ball " + objJoint.ball + " is not known in hash tree"
					}
				);
			}
			if ( rows[ 0 ].unit !== objUnit.unit )
			{
				return callback
				(
					_validation_utils.createJointError( "ball " + objJoint.ball + " unit " + objUnit.unit + " contradicts hash tree" )
				);
			}

			//	...
			conn.query
			(
				"SELECT ball FROM hash_tree_balls WHERE unit IN(?) \n\
				UNION \n\
				SELECT ball FROM balls WHERE unit IN(?) \n\
				ORDER BY ball",
				[
					objUnit.parent_units,
					objUnit.parent_units
				],
				function( prows )
				{
					var arrParentBalls;

					if ( prows.length !== objUnit.parent_units.length )
					{
						//	while the child is found in hash tree
						return callback
						(
							_validation_utils.createJointError( "some parents not found in balls nor in hash tree" )
						);
					}

					//	...
					arrParentBalls = prows.map
					(
						function( prow )
						{
							return prow.ball;
						}
					);
					if ( ! objJoint.skiplist_units )
					{
						return validateBallHash();
					}

					//	...
					conn.query
					(
						"SELECT ball FROM hash_tree_balls WHERE unit IN(?) \n\
						UNION \n\
						SELECT ball FROM balls WHERE unit IN(?) \n\
						ORDER BY ball",
						[
							objJoint.skiplist_units,
							objJoint.skiplist_units
						],
						function( srows )
						{
							if ( srows.length !== objJoint.skiplist_units.length )
							{
								return callback
								(
									_validation_utils.createJointError( "some skiplist balls not found" )
								);
							}

							//	...
							objValidationState.arrSkiplistBalls	= srows.map
							(
								function( srow )
								{
									return srow.ball;
								}
							);

							//	...
							validateBallHash();
						}
					);

					function validateBallHash()
					{
						var hash;

						//	...
						hash = _object_hash.getBallHash
						(
							objUnit.unit,
							arrParentBalls,
							objValidationState.arrSkiplistBalls,
							!! objUnit.content_hash
						);

						if ( hash !== objJoint.ball )
						{
							return callback( _validation_utils.createJointError( "ball hash is wrong" ) );
						}

						//	...
						callback();
					}
				}
			);
		}
	);
}


/**
 *	we cannot verify that skiplist units lie on MC if they are unstable yet,
 *	but if they don't, we'll get unmatching ball hash when the current unit reaches stability
 */
function _validateSkipList( conn, arrSkipListUnits, callback )
{
	var prev = "";

	//	...
	_async.eachSeries
	(
		arrSkipListUnits,
		function( sSkipListUnit, cb )
		{
			//	if (sSkipListUnit.charAt(0) !== "0")
			//		return cb("skiplist unit doesn't start with 0");
			if ( sSkipListUnit <= prev )
			{
				return cb( _validation_utils.createJointError( "skiplist units not ordered" ) );
			}

			//	...
			conn.query
			(
				"SELECT unit, is_stable, is_on_main_chain, main_chain_index FROM units WHERE unit=?",
				[
					sSkipListUnit
				],
				function( rows )
				{
					var objSkipListUnitProps;

					if ( rows.length === 0 )
					{
						return cb( "skiplist unit " + sSkipListUnit + " not found" );
					}

					//	...
					objSkipListUnitProps	= rows[ 0 ];

					//	if not stable, can't check that it is on MC as MC is not stable in its area yet
					if ( objSkipListUnitProps.is_stable === 1 )
					{
						if ( objSkipListUnitProps.is_on_main_chain !== 1 )
						{
							return cb( "skiplist unit " + sSkipListUnit + " is not on MC" );
						}
						if ( objSkipListUnitProps.main_chain_index % 10 !== 0 )
						{
							return cb( "skiplist unit " + sSkipListUnit + " MCI is not divisible by 10" );
						}
					}

					//
					//	we can't verify the choice of skiplist unit.
					//	If we try to find a skiplist unit now, we might find something matching on unstable part of MC.
					//	Again, we have another check when we reach stability
					//
					cb();
				}
			);
		},
		callback
	);
}


function _validateParents( conn, objJoint, objValidationState, callback )
{
	return ( new _validation_validate_parents.CValidateParents
	(
		conn,
		objJoint,
		objValidationState,
		callback

	) ).handle();
}

function _validateWitnesses( conn, objUnit, objValidationState, callback )
{
	return ( new _validation_validate_witnesses.CValidateWitnesses
	(
		conn,
		objUnit,
		objValidationState,
		callback

	) ).handle();
}

function _validateAuthors( conn, arrAuthors, objUnit, objValidationState, callback )
{
	return ( new _validation_validate_authors.CValidateAuthors
	(
		conn,
		arrAuthors,
		objUnit,
		objValidationState,
		callback

	) ).handle();
}

function _validateMessages( conn, arrMessages, objUnit, objValidationState, callback )
{
	return ( new _validation_validate_messages.CValidateMessages
	(
		conn,
		arrMessages,
		objUnit,
		objValidationState,
		callback

	) ).handle();
}




function _validateHeadersCommissionRecipients( objUnit, cb )
{
	var total_earned_headers_commission_share;
	var prev_address;
	var recipient;
	var i;

	if ( objUnit.authors.length > 1 &&
		typeof objUnit.earned_headers_commission_recipients !== "object" )
	{
		return cb( "must specify earned_headers_commission_recipients when more than 1 author" );
	}

	if ( "earned_headers_commission_recipients" in objUnit )
	{
		if ( ! _validation_utils.isNonemptyArray( objUnit.earned_headers_commission_recipients ) )
		{
			return cb( "empty earned_headers_commission_recipients array" );
		}

		//	...
		total_earned_headers_commission_share	= 0;
		prev_address				= "";

		for ( i = 0; i < objUnit.earned_headers_commission_recipients.length; i ++ )
		{
			//	...
			recipient = objUnit.earned_headers_commission_recipients[ i ];

			if ( ! _validation_utils.isPositiveInteger( recipient.earned_headers_commission_share ) )
			{
				return cb( "earned_headers_commission_share must be positive integer" );
			}
			if ( _validation_utils.hasFieldsExcept( recipient, [ "address", "earned_headers_commission_share" ] ) )
			{
				return cb( "unknowsn fields in recipient" );
			}
			if ( recipient.address <= prev_address )
			{
				return cb( "recipient list must be sorted by address" );
			}
			if ( ! _validation_utils.isValidAddress( recipient.address ) )
			{
				return cb( "invalid recipient address checksum" );
			}

			//	...
			total_earned_headers_commission_share	+= recipient.earned_headers_commission_share;
			prev_address				= recipient.address;
		}

		if ( total_earned_headers_commission_share !== 100 )
		{
			return cb( "sum of earned_headers_commission_share is not 100" );
		}
	}

	//
	//	...
	//
	cb();
}






/**
 *	exports
 */
exports.validate					= validate;
exports.hasValidHashes					= hasValidHashes;
exports.validateAuthorSignaturesWithoutReferences	= validateAuthorSignaturesWithoutReferences;
exports.validatePayment					= validatePayment;
exports.initPrivatePaymentValidationState		= initPrivatePaymentValidationState;
exports.validateSignedMessage				= validateSignedMessage;
exports.validateSignedMessageSync			= validateSignedMessageSync;
