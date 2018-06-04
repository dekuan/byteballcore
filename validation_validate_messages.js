/*jslint node: true */
"use strict";

var _async				= require( 'async' );
var _log				= require( './log.js' );
var _storage				= require( './storage.js' );
var _object_hash			= require( './object_hash.js' );
var _constants				= require( './constants.js' );
var _definition				= require( './definition.js' );
var _profiler_ex			= require( './profilerex.js' );

var _validation_utils			= require( './validation_utils.js' );
var _validation_check_for_double_spends	= require( './validation_check_for_double_spends.js' );
var _validation_validate_payment	= require( './validation_validate_payment.js' );




/**
 *	CValidateMessages
 *
 *	@param	conn_
 *	@param	arrMessages_
 *	@param	objUnit_
 *	@param	objValidationState_
 *	@param	callback_
 *	@constructor
 */
function CValidateMessages( conn_, arrMessages_, objUnit_, objValidationState_, callback_ )
{
	/**
	 *	handle
	 *	@public
	 */
	this.handle = function()
	{
		_log.consoleLog( "validateMessages " + objUnit_.unit );

		//	PPP
		_profiler_ex.begin( 'validation-validateMessages-async.forEachOfSeries' );

		//	...
		_async.forEachOfSeries
		(
			arrMessages_,
			function( objMessage, nMessageIndex, cb )
			{
				//	PPP
				_profiler_ex.begin( 'validation-validateMessages-validateMessage[' + String( arrMessages_.length ) + ']->' + String( nMessageIndex ) );

				//	...
				( new CValidateMessage(
					conn_,
					objMessage,
					nMessageIndex,
					objUnit_,
					objValidationState_,
					function ()
					{
						//	PPP
						_profiler_ex.end( 'validation-validateMessages-validateMessage[' + String( arrMessages_.length ) + ']->' + String( nMessageIndex ) );

						//
						//	TODO
						//	the parameter 1 might be invalid
						//	##########
						//
						cb.apply( this, arguments );
					}
				) ).handle();
			},
			function( err )
			{
				//	PPP
				_profiler_ex.end( 'validation-validateMessages-async.forEachOfSeries' );

				//	...
				if ( err )
				{
					return _callback( err );
				}
				if ( ! objValidationState_.bHasBasePayment )
				{
					return _callback( "no base payment message" );
				}

				//	...
				_callback();
			}
		);
	};


	//	--------------------------------------------------------------------------------
	//	Private
	//	--------------------------------------------------------------------------------

	function _constructor()
	{
	}


	/**
	 *	call back
	 *
	 *	@param	vError
	 *	@returns {*}
	 *	@private
	 */
	function _callback( vError )
	{
		if ( vError )
		{
			console.log( "CValidateMessages::_callback", vError );
		}
		else
		{
			console.log( "CValidateMessages::_callback - @successfully" );
		}

		//	...
		return callback_.apply( this, arguments );
	}



	//
	//	...
	//
	_constructor();
}


/**
 *	CValidateMessage
 *	***
 *	the slowest function in whole project
 */
function CValidateMessage( conn_, objMessage_, nMessageIndex_, objUnit_, objValidationState_, callback_ )
{
	/**
	 *	handle
	 *	@public
	 */
	this.handle = function()
	{
		var arrAuthorAddresses;
		var i;
		var objSpendProof;
		var address;
		var arrInlineOnlyApps;


		if ( typeof objMessage_.app !== "string" )
		{
			return _callback( "no app" );
		}
		if ( ! _validation_utils.isStringOfLength( objMessage_.payload_hash, _constants.HASH_LENGTH ) )
		{
			return _callback( "wrong payload hash size" );
		}
		if ( typeof objMessage_.payload_location !== "string" )
		{
			return _callback( "no payload_location" );
		}
		if ( _validation_utils.hasFieldsExcept( objMessage_,
			[ "app", "payload_hash", "payload_location", "payload", "payload_uri", "payload_uri_hash", "spend_proofs" ] ) )
		{
			return _callback( "unknown fields in message" );
		}

		//	...
		if ( "spend_proofs" in objMessage_ )
		{
			if ( ! Array.isArray( objMessage_.spend_proofs ) ||
				objMessage_.spend_proofs.length === 0 ||
				objMessage_.spend_proofs.length > _constants.MAX_SPEND_PROOFS_PER_MESSAGE )
			{
				return _callback( "spend_proofs must be non-empty array max " + _constants.MAX_SPEND_PROOFS_PER_MESSAGE + " elements" );
			}

			//	...
			arrAuthorAddresses = objUnit_.authors.map
			(
				function( author )
				{
					return author.address;
				}
			);

			//
			//	spend proofs are sorted in the same order as their corresponding inputs
			//	var prev_spend_proof = "";
			//
			for ( i = 0; i < objMessage_.spend_proofs.length; i++ )
			{
				objSpendProof	= objMessage_.spend_proofs[ i ];
				if ( typeof objSpendProof !== "object" )
				{
					return _callback( "spend_proof must be object" );
				}
				if ( _validation_utils.hasFieldsExcept( objSpendProof, [ "spend_proof", "address" ] ) )
				{
					return _callback( "unknown fields in spend_proof" );
				}

				//if (objSpendProof.spend_proof <= prev_spend_proof)
				//    return _callback("spend_proofs not sorted");

				if ( ! _validation_utils.isValidBase64( objSpendProof.spend_proof, _constants.HASH_LENGTH ) )
				{
					return _callback( "spend proof " + objSpendProof.spend_proof + " is not a valid base64" );
				}

				//	...
				address	= null;
				if ( arrAuthorAddresses.length === 1 )
				{
					if ( "address" in objSpendProof )
					{
						return _callback( "when single-authored, must not put address in spend proof" );
					}

					//	...
					address	= arrAuthorAddresses[ 0 ];
				}
				else
				{
					if ( typeof objSpendProof.address !== "string" )
					{
						return _callback( "when multi-authored, must put address in spend_proofs" );
					}
					if ( arrAuthorAddresses.indexOf( objSpendProof.address ) === -1 )
					{
						return _callback( "spend proof address " + objSpendProof.address + " is not an author" );
					}

					//	...
					address = objSpendProof.address;
				}

				if ( objValidationState_.arrInputKeys.indexOf( objSpendProof.spend_proof ) >= 0 )
				{
					return _callback( "spend proof " + objSpendProof.spend_proof + " already used" );
				}

				//	...
				objValidationState_.arrInputKeys.push( objSpendProof.spend_proof );
				//prev_spend_proof = objSpendProof.spend_proof;
			}

			//	...
			if ( objMessage_.payload_location === "inline" )
			{
				return _callback( "you don't need spend proofs when you have inline payload" );
			}
		}

		if ( objMessage_.payload_location !== "inline" &&
			objMessage_.payload_location !== "uri" &&
			objMessage_.payload_location !== "none" )
		{
			return _callback( "wrong payload location: " + objMessage_.payload_location );
		}

		if ( objMessage_.payload_location === "none" &&
			( "payload" in objMessage_ || "payload_uri" in objMessage_ || "payload_uri_hash" in objMessage_ ) )
		{
			return _callback( "must be no payload" );
		}

		if ( objMessage_.payload_location === "uri" )
		{
			if ( "payload" in objMessage_ )
			{
				return _callback( "must not contain payload" );
			}
			if ( typeof objMessage_.payload_uri !== "string" )
			{
				return _callback( "no payload uri" );
			}
			if ( ! _validation_utils.isStringOfLength( objMessage_.payload_uri_hash, _constants.HASH_LENGTH ) )
			{
				return _callback( "wrong length of payload uri hash" );
			}
			if ( objMessage_.payload_uri.length > 500 )
			{
				return _callback( "payload_uri too long" );
			}
			if ( _object_hash.getBase64Hash( objMessage_.payload_uri ) !== objMessage_.payload_uri_hash )
			{
				return _callback( "wrong payload_uri hash" );
			}
		}
		else
		{
			if ( "payload_uri" in objMessage_ || "payload_uri_hash" in objMessage_ )
			{
				return _callback( "must not contain payload_uri and payload_uri_hash" );
			}
		}

		if ( objMessage_.app === "payment" )
		{
			//	special requirements for payment
			if ( objMessage_.payload_location !== "inline" && objMessage_.payload_location !== "none" )
			{
				return _callback( "payment location must be inline or none" );
			}
			if ( objMessage_.payload_location === "none" && ! objMessage_.spend_proofs )
			{
				return _callback( "private payment must come with spend proof(s)" );
			}
		}

		//	...
		arrInlineOnlyApps =
			[
				"address_definition_change",
				"data_feed",
				"definition_template",
				"asset",
				"asset_attestors",
				"attestation",
				"poll",
				"vote"
			];
		if ( arrInlineOnlyApps.indexOf( objMessage_.app ) >= 0 && objMessage_.payload_location !== "inline" )
		{
			return _callback( objMessage_.app + " must be inline" );
		}


		//	PPP
		_profiler_ex.begin( "validation-validateMessages-bottom-async.series" );

		//	...
		_async.series
		(
			[
				_validateSpendProofs,
				_validatePayload
			],
			function ()
			{
				//	PPP
				_profiler_ex.end( "validation-validateMessages-bottom-async.series" );

				//
				//	TODO
				//	the parameter 1 might be invalid
				//	##########
				//
				_callback.apply( this, arguments );
			}
		);
	};



	//	--------------------------------------------------------------------------------
	//	Private
	//	--------------------------------------------------------------------------------

	function _constructor()
	{
	}


	//
	//	...
	//
	function _validatePayload( cb )
	{
		if ( objMessage_.payload_location === "inline" )
		{
			//	PPP
			_profiler_ex.begin( "validation-validateMessages-_validatePayload-validateInlinePayload" );

			//	...
			_validateInlinePayload
			(
				conn_,
				objMessage_,
				nMessageIndex_,
				objUnit_,
				objValidationState_,
				function ()
				{
					//	PPP
					_profiler_ex.end( "validation-validateMessages-_validatePayload-validateInlinePayload" );

					//
					//	TODO
					//	the parameter 1 might be invalid
					//	##########
					//
					cb.apply( this, arguments );
				}
			);
		}
		else
		{
			if ( ! _validation_utils.isValidBase64( objMessage_.payload_hash, _constants.HASH_LENGTH ) )
			{
				return cb( "wrong payload hash" );
			}

			//	...
			cb();
		}
	}

	function _validateSpendProofs( cb )
	{
		var arrEqs;

		if ( ! ( "spend_proofs" in objMessage_ ) )
		{
			return cb();
		}

		//	...
		arrEqs = objMessage_.spend_proofs.map
		(
			function( objSpendProof )
			{
				return "spend_proof=" + conn_.escape( objSpendProof.spend_proof )
					+ " AND address=" + conn_.escape
					(
						objSpendProof.address
							? objSpendProof.address
							: objUnit_.authors[ 0 ].address
					);
			}
		);

		//	...
		_validation_check_for_double_spends.checkForDoubleSpends
		(
			conn_,
			"spend proof",
			"SELECT address, unit, main_chain_index, sequence " +
			"FROM spend_proofs JOIN units USING(unit) " +
			"WHERE unit != ? AND (" + arrEqs.join( " OR " ) + ") ",
			[
				objUnit_.unit
			],
			objUnit_,
			objValidationState_,
			function( cb2 )
			{
				cb2();
			},
			cb
		);
	}


	function _validateInlinePayload( conn, objMessage, message_index, objUnit, objValidationState, callback )
	{
		var payload;
		var address;
		var i;
		var feed_name;
		var value;

		//	...
		payload = objMessage.payload;

		if ( typeof payload === "undefined" )
		{
			return callback( "no inline payload" );
		}
		if ( _object_hash.getBase64Hash( payload ) !== objMessage.payload_hash )
		{
			return callback( "wrong payload hash: expected " + _object_hash.getBase64Hash( payload ) + ", got " + objMessage.payload_hash );
		}

		switch ( objMessage.app )
		{
			case "text":
				if ( typeof payload !== "string" )
				{
					return callback( "payload must be string" );
				}

				return callback();

			case "address_definition_change":
				if ( _validation_utils.hasFieldsExcept( payload, [ "definition_chash", "address" ] ) )
				{
					return callback( "unknown fields in address_definition_change" );
				}

				var arrAuthorAddresses = objUnit.authors.map
				(
					function( author )
					{
						return author.address;
					}
				);

				//	...
				if ( objUnit.authors.length > 1 )
				{
					if ( ! _validation_utils.isValidAddress( payload.address ) )
					{
						return callback( "when multi-authored, must indicate address" );
					}
					if ( arrAuthorAddresses.indexOf( payload.address ) === -1 )
					{
						return callback( "foreign address" );
					}

					//	...
					address = payload.address;
				}
				else
				{
					if ( 'address' in payload )
					{
						return callback( "when single-authored, must not indicate address" );
					}

					//	...
					address	= arrAuthorAddresses[ 0 ];
				}

				if ( ! objValidationState.arrDefinitionChangeFlags )
				{
					objValidationState.arrDefinitionChangeFlags = {};
				}
				if ( objValidationState.arrDefinitionChangeFlags[ address ] )
				{
					return callback( "can be only one definition change per address" );
				}

				objValidationState.arrDefinitionChangeFlags[ address ] = true;
				if ( ! _validation_utils.isValidAddress( payload.definition_chash ) )
				{
					return callback( "bad new definition_chash" );
				}

				//	...
				return callback();

			case "poll":
				if ( objValidationState.bHasPoll )
				{
					return callback( "can be only one poll" );
				}

				objValidationState.bHasPoll	= true;
				if ( typeof payload !== "object" || Array.isArray( payload ) )
				{
					return callback( "poll payload must be object" );
				}
				if ( _validation_utils.hasFieldsExcept( payload, [ "question", "choices" ] ) )
				{
					return callback( "unknown fields in " + objMessage.app );
				}
				if ( typeof payload.question !== 'string' )
				{
					return callback( "no question in poll" );
				}
				if ( ! _validation_utils.isNonemptyArray( payload.choices ) )
				{
					return callback( "no choices in poll" );
				}
				if ( payload.choices.length > _constants.MAX_CHOICES_PER_POLL )
				{
					return callback( "too many choices in poll" );
				}

				for ( i = 0; i < payload.choices.length; i++ )
				{
					if ( typeof payload.choices[ i ] !== 'string' )
					{
						return callback( "all choices must be strings" );
					}
				}

				//	...
				return callback();

			case "vote":
				if ( ! _validation_utils.isStringOfLength( payload.unit, _constants.HASH_LENGTH ) )
				{
					return callback( "invalid unit in vote" );
				}
				if ( typeof payload.choice !== "string" )
				{
					return callback( "choice must be string" );
				}
				if ( _validation_utils.hasFieldsExcept( payload, [ "unit", "choice" ] ) )
				{
					return callback( "unknown fields in " + objMessage.app );
				}

				//	...
				conn.query
				(
					"SELECT main_chain_index, sequence FROM polls JOIN poll_choices USING(unit) JOIN units USING(unit) WHERE unit=? AND choice=?",
					[
						payload.unit,
						payload.choice
					],
					function( poll_unit_rows )
					{
						if ( poll_unit_rows.length > 1 )
						{
							throw Error( "more than one poll?" );
						}
						if ( poll_unit_rows.length === 0 )
						{
							return callback( "invalid choice " + payload.choice + " or poll " + payload.unit );
						}

						//	...
						var objPollUnitProps	= poll_unit_rows[ 0 ];
						if ( objPollUnitProps.main_chain_index === null ||
							objPollUnitProps.main_chain_index > objValidationState.last_ball_mci )
						{
							return callback( "poll unit must be before last ball" );
						}
						if ( objPollUnitProps.sequence !== 'good' )
						{
							return callback( "poll unit is not serial" );
						}

						//	...
						return callback();
					}
				);
				break;

			case "data_feed":
				if ( objValidationState.bHasDataFeed )
				{
					return callback( "can be only one data feed" );
				}

				//	...
				objValidationState.bHasDataFeed	= true;
				if ( typeof payload !== "object" || Array.isArray( payload ) || Object.keys( payload ).length === 0 )
				{
					return callback( "data feed payload must be non-empty object" );
				}

				for ( feed_name in payload )
				{
					if ( ! payload.hasOwnProperty( feed_name ) )
					{
						continue;
					}

					//	...
					if ( feed_name.length > _constants.MAX_DATA_FEED_NAME_LENGTH )
					{
						return callback( "feed name " + feed_name + " too long" );
					}

					//	...
					value	= payload[ feed_name ];
					if ( typeof value === 'string' )
					{
						if ( value.length > _constants.MAX_DATA_FEED_VALUE_LENGTH )
						{
							return callback( "value " + value + " too long" );
						}
					}
					else if ( typeof value === 'number' )
					{
						if ( ! _validation_utils.isInteger( value ) )
						{
							return callback( "fractional numbers not allowed in data feeds" );
						}
					}
					else
					{
						return callback( "data feed " + feed_name + " must be string or number" );
					}
				}

				//	...
				return callback();

			case "profile" :
				if ( objUnit.authors.length !== 1 )
				{
					return callback( "profile must be single-authored" );
				}
				if ( objValidationState.bHasProfile )
				{
					return callback( "can be only one profile" );
				}

				//	...
				objValidationState.bHasProfile = true;

				// no break, continuing

			case "data":
				if ( typeof payload !== "object" || payload === null )
				{
					return callback( objMessage.app + " payload must be object" );
				}

				//	...
				return callback();

			case "definition_template":
				if ( objValidationState.bHasDefinitionTemplate )
				{
					return callback( "can be only one definition template" );
				}

				//	...
				objValidationState.bHasDefinitionTemplate	= true;
				if ( ! _validation_utils.isArrayOfLength( payload, 2 ) )
				{
					return callback( objMessage.app + " payload must be array of two elements" );
				}

				//	...
				return callback();

			case "attestation":
				if ( objUnit.authors.length !== 1 )
				{
					return callback( "attestation must be single-authored" );
				}
				if ( _validation_utils.hasFieldsExcept( payload, [ "address", "profile" ] ) )
				{
					return callback( "unknown fields in " + objMessage.app );
				}
				if ( ! _validation_utils.isValidAddress( payload.address ) )
				{
					return callback( "attesting an invalid address" );
				}
				if ( typeof payload.profile !== 'object' || payload.profile === null )
				{
					return callback( "attested profile must be object" );
				}

				//
				//	it is ok if the address has never been used yet
				//	it is also ok to attest oneself
				return callback();

			case "asset":
				if ( objValidationState.bHasAssetDefinition )
				{
					return callback( "can be only one asset definition" );
				}

				//	...
				objValidationState.bHasAssetDefinition = true;
				_validateAssetDefinition( conn, payload, objUnit, objValidationState, callback );
				break;

			case "asset_attestors":
				if ( ! objValidationState.assocHasAssetAttestors )
				{
					objValidationState.assocHasAssetAttestors = {};
				}
				if ( objValidationState.assocHasAssetAttestors[ payload.asset ] )
				{
					return callback( "can be only one asset attestor list update per asset" );
				}

				//	...
				objValidationState.assocHasAssetAttestors[ payload.asset ] = true;
				_validateAssertorListUpdate
				(
					conn,
					payload,
					objUnit,
					objValidationState,
					callback
				);
				break;

			case "payment":
				_validation_validate_payment.validatePayment
				(
					conn,
					payload,
					message_index,
					objUnit,
					objValidationState,
					callback
				);
				break;

			default:
				return callback( "unknown app: " + objMessage.app );
		}
	}

	function _validateAssertorListUpdate( conn, payload, objUnit, objValidationState, callback )
	{
		if ( objUnit.authors.length !== 1 )
		{
			return callback( "attestor list must be single-authored" );
		}
		if ( ! _validation_utils.isStringOfLength( payload.asset, _constants.HASH_LENGTH ) )
		{
			return callback( "invalid asset in attestor list update" );
		}

		//	...
		_storage.readAsset
		(
			conn,
			payload.asset,
			objValidationState.last_ball_mci,
			function( err, objAsset )
			{
				if ( err )
				{
					return callback( err );
				}
				if ( ! objAsset.spender_attested )
				{
					return callback( "this asset does not require attestors" );
				}
				if ( objUnit.authors[ 0 ].address !== objAsset.definer_address )
				{
					return callback( "attestor list can be edited only by definer" );
				}

				//	...
				err = _checkAttestorList( payload.attestors );
				if ( err )
				{
					return callback( err );
				}

				//	...
				callback();
			}
		);
	}


	function _validateAssetDefinition( conn, payload, objUnit, objValidationState, callback )
	{
		var err;
		var total_cap_from_denominations;
		var bHasUncappedDenominations;
		var prev_denom;
		var i;
		var denomInfo;

		if ( objUnit.authors.length !== 1 )
		{
			return callback( "asset definition must be single-authored" );
		}
		if ( _validation_utils.hasFieldsExcept( payload,
			[ "cap", "is_private", "is_transferrable", "auto_destroy", "fixed_denominations", "issued_by_definer_only", "cosigned_by_definer", "spender_attested", "issue_condition", "transfer_condition", "attestors", "denominations" ] ) )
		{
			return callback( "unknown fields in asset definition" );
		}
		if ( typeof payload.is_private !== "boolean" ||
			typeof payload.is_transferrable !== "boolean" ||
			typeof payload.auto_destroy !== "boolean" ||
			typeof payload.fixed_denominations !== "boolean" ||
			typeof payload.issued_by_definer_only !== "boolean" ||
			typeof payload.cosigned_by_definer !== "boolean" ||
			typeof payload.spender_attested !== "boolean" )
		{
			return callback( "some required fields in asset definition are missing" );
		}
		if ( "cap" in payload &&
			! ( _validation_utils.isPositiveInteger( payload.cap ) &&
				payload.cap <= _constants.MAX_CAP ) )
		{
			return callback( "invalid cap" );
		}

		//	attestors
		if ( payload.spender_attested && ( err = _checkAttestorList( payload.attestors ) ) )
		{
			return callback( err );
		}

		//	denominations
		if ( payload.fixed_denominations && ! _validation_utils.isNonemptyArray( payload.denominations ) )
		{
			return callback( "denominations not defined" );
		}
		if ( payload.denominations )
		{
			if ( payload.denominations.length > _constants.MAX_DENOMINATIONS_PER_ASSET_DEFINITION )
			{
				return callback( "too many denominations" );
			}

			//	...
			total_cap_from_denominations	= 0;
			bHasUncappedDenominations	= false;
			prev_denom			= 0;

			for ( i = 0; i < payload.denominations.length; i ++ )
			{
				//	...
				denomInfo	= payload.denominations[ i ];
				if ( ! _validation_utils.isPositiveInteger( denomInfo.denomination ) )
				{
					return callback( "invalid denomination" );
				}
				if ( denomInfo.denomination <= prev_denom )
				{
					return callback( "denominations unsorted" );
				}
				if ( "count_coins" in denomInfo )
				{
					if ( ! _validation_utils.isPositiveInteger( denomInfo.count_coins ) )
					{
						return callback( "invalid count_coins" );
					}

					//	...
					total_cap_from_denominations += denomInfo.count_coins * denomInfo.denomination;
				}
				else
				{
					bHasUncappedDenominations = true;
				}

				//	...
				prev_denom = denomInfo.denomination;
			}

			if ( bHasUncappedDenominations && total_cap_from_denominations )
			{
				return callback( "some denominations are capped, some uncapped" );
			}
			if ( bHasUncappedDenominations && payload.cap )
			{
				return callback( "has cap but some denominations are uncapped" );
			}
			if ( total_cap_from_denominations && !payload.cap )
			{
				return callback( "has no cap but denominations are capped" );
			}
			if ( total_cap_from_denominations && payload.cap !== total_cap_from_denominations )
			{
				return callback( "cap doesn't match sum of denominations" );
			}
		}

		if ( payload.is_private &&
			payload.is_transferrable &&
			! payload.fixed_denominations )
		{
			return callback( "if private and transferrable, must have fixed denominations" );
		}
		if ( payload.is_private &&
			! payload.fixed_denominations )
		{
			if ( ! ( payload.auto_destroy && ! payload.is_transferrable ) )
			{
				return callback( "if private and divisible, must also be auto-destroy and non-transferrable" );
			}
		}
		if ( payload.cap && !payload.issued_by_definer_only )
		{
			return callback( "if capped, must be issued by definer only" );
		}

		//
		//	possible: definer is like black hole
		//	if (!payload.issued_by_definer_only && payload.auto_destroy)
		//		return callback("if issued by anybody, cannot auto-destroy");
		//

		//
		//	possible: the entire issue should go to the definer
		//	if (!payload.issued_by_definer_only && !payload.is_transferrable)
		//		return callback("if issued by anybody, must be transferrable");
		//

		objValidationState.bDefiningPrivateAsset = payload.is_private;

		//	...
		_async.series
		(
			[
				function( cb )
				{
					if ( ! ( "issue_condition" in payload ) )
					{
						return cb();
					}

					//	...
					_definition.validateDefinition
					(
						conn,
						payload.issue_condition,
						objUnit,
						objValidationState,
						null,
						true,
						cb
					);
				},
				function( cb )
				{
					if ( ! ( "transfer_condition" in payload ) )
					{
						return cb();
					}

					//	...
					_definition.validateDefinition
					(
						conn,
						payload.transfer_condition,
						objUnit,
						objValidationState,
						null,
						true,
						cb
					);
				}
			],
			callback
		);
	}


	function _checkAttestorList( arrAttestors )
	{
		var prev;
		var i;

		if ( ! _validation_utils.isNonemptyArray( arrAttestors ) )
		{
			return "attestors not defined";
		}
		if ( arrAttestors.length > _constants.MAX_ATTESTORS_PER_ASSET )
		{
			return "too many attestors";
		}

		//	...
		prev	= "";

		for ( i = 0; i < arrAttestors.length; i++ )
		{
			if ( arrAttestors[i] <= prev )
			{
				return "attestors not sorted";
			}
			if ( ! _validation_utils.isValidAddress( arrAttestors[ i ] ) )
			{
				return "invalid attestor address: " + arrAttestors[ i ];
			}

			//	...
			prev = arrAttestors[ i ];
		}

		return null;
	}


	/**
	 *	call back
	 *
	 *	@param	vError
	 *	@returns {*}
	 *	@private
	 */
	function _callback( vError )
	{
		if ( vError )
		{
			console.log( "CValidateMessage::_callback", vError );
		}
		else
		{
			console.log( "CValidateMessage::_callback - @successfully" );
		}

		//	...
		return callback_.apply( this, arguments );
	}



	//
	//	...
	//
	_constructor();
}





/**
 *	exports
 */
exports.CValidateMessages	= CValidateMessages;

