/*jslint node: true */
"use strict";

var _async				= require( 'async' );
var _log				= require( './log.js' );
var _storage				= require( './storage.js' );
var _graph				= require( './graph.js' );
var _paid_witnessing			= require( './paid_witnessing.js' );
var _headers_commission			= require( './headers_commission.js' );
var _mc_outputs				= require( './mc_outputs.js' );
var _mutex				= require( './mutex.js' );
var _constants				= require( './constants.js' );
var _definition				= require( './definition.js' );
var _conf				= require( './conf.js' );

var _validation_utils			= require( './validation_utils.js' );
var _validation_check_for_double_spends	= require( './validation_check_for_double_spends.js' );





/**
 *	@public
 *
 *	used for both public and private payments
 */
function validatePayment( conn, payload, message_index, objUnit, objValidationState, callback )
{
	if ( ! ( "asset" in payload ) )
	{
		//	base currency
		if ( _validation_utils.hasFieldsExcept( payload, [ "inputs", "outputs" ] ) )
		{
			return callback( "unknown fields in payment message" );
		}
		if ( objValidationState.bHasBasePayment )
		{
			return callback( "can have only one base payment" );
		}

		//	...
		objValidationState.bHasBasePayment = true;
		return _validatePaymentInputsAndOutputs( conn, payload, null, message_index, objUnit, objValidationState, callback );
	}

	//	asset
	if ( ! _validation_utils.isStringOfLength( payload.asset, _constants.HASH_LENGTH ) )
	{
		return callback( "invalid asset" );
	}

	//	...
	var arrAuthorAddresses = objUnit.authors.map
	(
		function( author )
		{
			return author.address;
		}
	);

	//	note that light clients cannot check attestations
	_storage.loadAssetWithListOfAttestedAuthors
	(
		conn,
		payload.asset,
		objValidationState.last_ball_mci,
		arrAuthorAddresses,
		function( err, objAsset )
		{
			if ( err )
			{
				return callback( err );
			}

			if ( _validation_utils.hasFieldsExcept( payload, [ "inputs", "outputs", "asset", "denomination" ] ) )
			{
				return callback( "unknown fields in payment message" );
			}
			if ( ! _validation_utils.isNonemptyArray( payload.inputs ) )
			{
				return callback( "no inputs" );
			}
			if ( ! _validation_utils.isNonemptyArray( payload.outputs ) )
			{
				return callback( "no outputs" );
			}
			if ( objAsset.fixed_denominations )
			{
				if ( ! _validation_utils.isPositiveInteger( payload.denomination ) )
				{
					return callback( "no denomination" );
				}
			}
			else
			{
				if ( "denomination" in payload )
				{
					return callback( "denomination in arbitrary-amounts asset" );
				}
			}

			if ( !! objAsset.is_private !== !! objValidationState.bPrivate )
			{
				return callback( "asset privacy mismatch" );
			}

			//	...
			var bIssue	= ( payload.inputs[ 0 ].type === "issue" );
			var issuer_address;

			if ( bIssue )
			{
				if ( arrAuthorAddresses.length === 1 )
				{
					issuer_address	= arrAuthorAddresses[ 0 ];
				}
				else
				{
					issuer_address	= payload.inputs[ 0 ].address;
					if ( arrAuthorAddresses.indexOf( issuer_address ) === -1 )
					{
						return callback( "issuer not among authors" );
					}
				}

				if ( objAsset.issued_by_definer_only && issuer_address !== objAsset.definer_address )
				{
					return callback( "only definer can issue this asset" );
				}
			}

			if ( objAsset.cosigned_by_definer && arrAuthorAddresses.indexOf( objAsset.definer_address ) === -1 )
			{
				return callback( "must be cosigned by definer" );
			}

			if ( objAsset.spender_attested )
			{
				if ( _conf.bLight && objAsset.is_private )
				{
					//
					//	in light clients,
					//	we don't have the attestation data but if the asset is public,
					// 	we trust witnesses to have checked attestations
					//
					//	TODO:
					//	request history
					return callback( "being light, I can't check attestations for private assets" );
				}
				if ( objAsset.arrAttestedAddresses.length === 0 )
				{
					return callback( "none of the authors is attested" );
				}
				if ( bIssue && objAsset.arrAttestedAddresses.indexOf( issuer_address ) === -1 )
				{
					return callback( "issuer is not attested" );
				}
			}

			//	...
			_validatePaymentInputsAndOutputs
			(
				conn,
				payload,
				objAsset,
				message_index,
				objUnit,
				objValidationState,
				callback
			);
		}
	);
}


/**
 *	divisible assets (including base asset)
 */
function _validatePaymentInputsAndOutputs( conn, payload, objAsset, message_index, objUnit, objValidationState, callback )
{
//	if (objAsset)
//		profiler2.start();
	var denomination	= payload.denomination || 1;
	var arrAuthorAddresses	= objUnit.authors.map( function( author ) { return author.address; } );
	var arrInputAddresses	= []; // used for non-transferrable assets only
	var arrOutputAddresses	= [];
	var total_input		= 0;

	//	...
	var total_output	= 0;
	var prev_address	= "";	//	if public, outputs must be sorted by address
	var prev_amount		= 0;
	var count_open_outputs	= 0;

	var i;
	var output;


	if ( payload.inputs.length > _constants.MAX_INPUTS_PER_PAYMENT_MESSAGE )
	{
		return callback( "too many inputs" );
	}
	if ( payload.outputs.length > _constants.MAX_OUTPUTS_PER_PAYMENT_MESSAGE )
	{
		return callback( "too many outputs" );
	}
	if ( objAsset && objAsset.fixed_denominations && payload.inputs.length !== 1 )
	{
		return callback( "fixed denominations payment must have 1 input" );
	}

	//	...
	for ( i = 0; i < payload.outputs.length; i ++ )
	{
		//	...
		output	= payload.outputs[ i ];

		if ( _validation_utils.hasFieldsExcept( output, [ "address", "amount", "blinding", "output_hash" ] ) )
		{
			return callback( "unknown fields in payment output" );
		}
		if ( ! _validation_utils.isPositiveInteger( output.amount ) )
		{
			return callback( "amount must be positive integer, found " + output.amount );
		}
		if ( objAsset && objAsset.fixed_denominations && output.amount % denomination !== 0 )
		{
			return callback( "output amount must be divisible by denomination" );
		}

		if ( objAsset && objAsset.is_private )
		{
			if ( ( "output_hash" in output ) !== !! objAsset.fixed_denominations )
			{
				return callback( "output_hash must be present with fixed denominations only" );
			}
			if ( "output_hash" in output &&
				! _validation_utils.isStringOfLength( output.output_hash, _constants.HASH_LENGTH ) )
			{
				return callback( "invalid output hash" );
			}
			if ( ! objAsset.fixed_denominations &&
				! ( ( "blinding" in output ) && ( "address" in output ) ) )
			{
				return callback( "no blinding or address" );
			}
			if ( "blinding" in output &&
				! _validation_utils.isStringOfLength( output.blinding, 16 ) )
			{
				return callback( "bad blinding" );
			}
			if ( ( "blinding" in output ) !== ( "address" in output ) )
			{
				return callback( "address and blinding must come together" );
			}
			if ( "address" in output &&
				! _validation_utils.isValidAddressAnyCase( output.address ) )
			{
				return callback( "output address " + output.address + " invalid" );
			}
			if ( output.address )
			{
				count_open_outputs ++;
			}
		}
		else
		{
			if ( "blinding" in output )
			{
				return callback( "public output must not have blinding" );
			}
			if ( "output_hash" in output )
			{
				return callback( "public output must not have output_hash" );
			}
			if ( ! _validation_utils.isValidAddressAnyCase( output.address ) )
			{
				return callback( "output address " + output.address + " invalid" );
			}
			if ( prev_address > output.address )
			{
				return callback( "output addresses not sorted" );
			}
			else if ( prev_address === output.address &&
				prev_amount > output.amount )
			{
				return callback( "output amounts for same address not sorted" );
			}

			//	...
			prev_address	= output.address;
			prev_amount	= output.amount;
		}

		if ( output.address &&
			arrOutputAddresses.indexOf( output.address ) === -1 )
		{
			arrOutputAddresses.push( output.address );
		}

		//	...
		total_output += output.amount;
	}

	if ( objAsset && objAsset.is_private &&
		count_open_outputs !== 1 )
	{
		return callback( "found " + count_open_outputs + " open outputs, expected 1" );
	}

	//	...
	var bIssue			= false;
	var bHaveHeadersComissions	= false;
	var bHaveWitnessings		= false;

	//	same for both public and private
	function validateIndivisibleIssue( input, cb )
	{
		//	if (objAsset)
		//		profiler2.start();
		conn.query
		(
			"SELECT count_coins FROM asset_denominations WHERE asset=? AND denomination=?",
			[
				payload.asset,
				denomination
			],
			function( rows )
			{
				var denomInfo;

				if ( rows.length === 0 )
				{
					return cb( "invalid denomination: " + denomination );
				}
				if ( rows.length > 1 )
				{
					throw Error( "more than one record per denomination?" );
				}

				//	...
				denomInfo	= rows[ 0 ];
				if ( denomInfo.count_coins === null )
				{
					//	uncapped
					if ( input.amount % denomination !== 0 )
					{
						return cb( "issue amount must be multiple of denomination" );
					}
				}
				else
				{
					if ( input.amount !== denomination * denomInfo.count_coins )
					{
						return cb( "wrong size of issue of denomination " + denomination );
					}
				}

				//	if (objAsset)
				//		profiler2.stop('validateIndivisibleIssue');
				cb();
			}
		);
	}

//	if (objAsset)
//		profiler2.stop('validate outputs');

	//
	//	max 1 issue must come first, then transfers, then hc, then witnessings
	//	no particular sorting order within the groups
	//
	_async.forEachOfSeries
	(
		payload.inputs,
		function( input, input_index, cb )
		{
			var type;
			var doubleSpendFields;
			var doubleSpendWhere;
			var doubleSpendVars;
			var address;
			var input_key;

			if ( objAsset )
			{
				if ( "type" in input && input.type !== "issue" )
				{
					return cb( "non-base input can have only type=issue" );
				}
			}
			else
			{
				if ( "type" in input && ! _validation_utils.isNonemptyString( input.type ) )
				{
					return cb( "bad input type" );
				}
			}

			//	...
			type			= input.type || "transfer";
			doubleSpendFields	= "unit, address, message_index, input_index, main_chain_index, sequence, is_stable";
			doubleSpendVars		= [];
			doubleSpendWhere	= "";


			//	...
			function checkInputDoubleSpend( cb2 )
			{
				//	if (objAsset)
				//		profiler2.start();
				doubleSpendWhere += " AND unit != " + conn.escape( objUnit.unit );

				if ( objAsset )
				{
					doubleSpendWhere += " AND asset=?";
					doubleSpendVars.push( payload.asset );
				}
				else
				{
					doubleSpendWhere	+= " AND asset IS NULL";
				}

				//	...
				var doubleSpendQuery	= "SELECT " + doubleSpendFields + " FROM inputs JOIN units USING(unit) WHERE " + doubleSpendWhere;
				_validation_check_for_double_spends.checkForDoubleSpends
				(
					conn,
					"divisible input",
					doubleSpendQuery,
					doubleSpendVars,
					objUnit,
					objValidationState,
					function acceptDoublespends( cb3 )
					{
						_log.consoleLog( "--- accepting doublespend on unit " + objUnit.unit );

						//	...
						var sql = "UPDATE inputs SET is_unique=NULL WHERE " + doubleSpendWhere +
							" AND (SELECT is_stable FROM units WHERE units.unit=inputs.unit)=0";
						if ( ! ( objAsset && objAsset.is_private ) )
						{
							objValidationState.arrAdditionalQueries.push
							(
								{
									sql	: sql,
									params	: doubleSpendVars
								}
							);
							objValidationState.arrDoubleSpendInputs.push
							(
								{
									message_index	: message_index,
									input_index	: input_index
								}
							);

							//	...
							return cb3();
						}

						//
						//	* IMPORTANT
						//
						_mutex.lock
						(
							[ "private_write" ],
							function( unlock )
							{
								_log.consoleLog( "--- will ununique the conflicts of unit " + objUnit.unit );

								//	...
								conn.query
								(
									sql,
									doubleSpendVars,
									function()
									{
										_log.consoleLog( "--- ununique done unit " + objUnit.unit );
										objValidationState.arrDoubleSpendInputs.push
										(
											{
												message_index	: message_index,
												input_index	: input_index
											}
										);

										//	...
										unlock();
										cb3();
									}
								);
							}
						);
					},
					function onDone( err )
					{
						if ( err && objAsset && objAsset.is_private )
						{
							throw Error( "spend proof didn't help: " + err );
						}

						//	if (objAsset)
						//		profiler2.stop('checkInputDoubleSpend');
						cb2( err );
					}
				);
			}

			//
			//	...
			//
			switch ( type )
			{
				case "issue":
					//	if (objAsset)
					//		profiler2.start();
					if ( input_index !== 0 )
					{
						return cb( "issue must come first" );
					}
					if ( _validation_utils.hasFieldsExcept( input, [ "type", "address", "amount", "serial_number" ] ) )
					{
						return cb( "unknown fields in issue input" );
					}
					if ( ! _validation_utils.isPositiveInteger( input.amount ) )
					{
						return cb( "amount must be positive" );
					}
					if ( ! _validation_utils.isPositiveInteger( input.serial_number ) )
					{
						return cb( "serial_number must be positive" );
					}
					if ( ! objAsset || objAsset.cap )
					{
						if ( input.serial_number !== 1 )
						{
							return cb( "for capped asset serial_number must be 1" );
						}
					}
					if ( bIssue )
					{
						return cb( "only one issue per message allowed" );
					}

					//	...
					bIssue	= true;
					address	= null;

					if ( arrAuthorAddresses.length === 1 )
					{
						if ( "address" in input )
						{
							return cb( "when single-authored, must not put address in issue input" );
						}

						//	...
						address = arrAuthorAddresses[ 0 ];
					}
					else
					{
						if ( typeof input.address !== "string" )
						{
							return cb( "when multi-authored, must put address in issue input" );
						}
						if ( arrAuthorAddresses.indexOf( input.address ) === -1 )
						{
							return cb( "issue input address " + input.address + " is not an author" );
						}

						//	...
						address	= input.address;
					}

					//	...
					arrInputAddresses = [ address ];
					if ( objAsset )
					{
						if ( objAsset.cap &&
							! objAsset.fixed_denominations &&
							input.amount !== objAsset.cap )
						{
							return cb( "issue must be equal to cap" );
						}
					}
					else
					{
						if ( ! _storage.isGenesisUnit( objUnit.unit ) )
						{
							return cb( "only genesis can issue base asset" );
						}
						if ( input.amount !== _constants.TOTAL_WHITEBYTES )
						{
							return cb( "issue must be equal to cap" );
						}
					}

					//	...
					total_input	+= input.amount;
					input_key	= ( payload.asset || "base" ) + "-" + denomination + "-" + address + "-" + input.serial_number;

					if ( objValidationState.arrInputKeys.indexOf( input_key ) >= 0 )
					{
						return callback( "input " + input_key + " already used" );
					}

					objValidationState.arrInputKeys.push( input_key );
					doubleSpendWhere	= "type='issue'";
					doubleSpendVars		= [];

					if ( objAsset && objAsset.fixed_denominations )
					{
						doubleSpendWhere += " AND denomination=?";
						doubleSpendVars.push( denomination );
					}
					if ( objAsset )
					{
						doubleSpendWhere += " AND serial_number=?";
						doubleSpendVars.push( input.serial_number );
					}
					if ( objAsset && ! objAsset.issued_by_definer_only )
					{
						doubleSpendWhere += " AND address=?";
						doubleSpendVars.push( address );
					}
					//	if (objAsset)
					//		profiler2.stop('validate issue');
					if ( objAsset && objAsset.fixed_denominations )
					{
						validateIndivisibleIssue
						(
							input,
							function( err )
							{
								if ( err )
								{
									return cb( err );
								}

								//	...
								checkInputDoubleSpend( cb );
							}
						);
					}
					else
					{
						checkInputDoubleSpend( cb );
					}

					//	attestations and issued_by_definer_only already checked before
					break;

				case "transfer":
					//	if (objAsset)
					//		profiler2.start();
					if ( bHaveHeadersComissions ||
						bHaveWitnessings )
					{
						return cb( "all transfers must come before hc and witnessings" );
					}
					if ( _validation_utils.hasFieldsExcept( input, [ "type", "unit", "message_index", "output_index" ] ) )
					{
						return cb( "unknown fields in payment input" );
					}
					if ( ! _validation_utils.isStringOfLength( input.unit, _constants.HASH_LENGTH ) )
					{
						return cb( "wrong unit length in payment input" );
					}
					if ( ! _validation_utils.isNonnegativeInteger( input.message_index ) )
					{
						return cb( "no message_index in payment input" );
					}
					if ( ! _validation_utils.isNonnegativeInteger( input.output_index ) )
					{
						return cb( "no output_index in payment input" );
					}

					//	...
					input_key = ( payload.asset || "base" ) + "-" + input.unit + "-" + input.message_index + "-" + input.output_index;

					if ( objValidationState.arrInputKeys.indexOf( input_key ) >= 0 )
					{
						return cb( "input " + input_key + " already used" );
					}

					//	...
					objValidationState.arrInputKeys.push( input_key );

					//	...
					doubleSpendWhere	= "type=? AND src_unit=? AND src_message_index=? AND src_output_index=?";
					doubleSpendVars		= [ type, input.unit, input.message_index, input.output_index ];

					//
					//	for private fixed denominations assets, we can't look up src output in the database
					//	because we validate the entire chain before saving anything.
					//	Instead we prepopulate objValidationState with denomination and src_output
					//
					if ( objAsset &&
						objAsset.is_private &&
						objAsset.fixed_denominations )
					{
						if ( ! objValidationState.src_coin )
						{
							throw Error( "no src_coin" );
						}

						var src_coin = objValidationState.src_coin;
						if ( ! src_coin.src_output )
						{
							throw Error( "no src_output" );
						}
						if ( ! _validation_utils.isPositiveInteger( src_coin.denomination ) )
						{
							throw Error( "no denomination in src coin" );
						}
						if ( ! _validation_utils.isPositiveInteger( src_coin.amount ) )
						{
							throw Error( "no src coin amount" );
						}

						var owner_address = src_coin.src_output.address;
						if ( arrAuthorAddresses.indexOf( owner_address ) === -1 )
						{
							return cb( "output owner is not among authors" );
						}
						if ( denomination !== src_coin.denomination )
						{
							return cb( "private denomination mismatch" );
						}
						if ( objAsset.auto_destroy &&
							owner_address === objAsset.definer_address )
						{
							return cb( "this output was destroyed by sending to definer address" );
						}
						if ( objAsset.spender_attested &&
							objAsset.arrAttestedAddresses.indexOf( owner_address ) === -1 )
						{
							return cb( "owner address is not attested" );
						}
						if ( arrInputAddresses.indexOf( owner_address ) === -1 )
						{
							arrInputAddresses.push( owner_address );
						}

						//	...
						total_input += src_coin.amount;
						_log.consoleLog( "-- val state " + JSON.stringify( objValidationState ) );
						//	if (objAsset)
						//		profiler2.stop('validate transfer');
						return checkInputDoubleSpend( cb );
					}

					//	...
					conn.query
					(
						"SELECT amount, is_stable, sequence, address, main_chain_index, denomination, asset \n\
						FROM outputs \n\
						JOIN units USING(unit) \n\
						WHERE outputs.unit=? AND message_index=? AND output_index=?",
						[
							input.unit,
							input.message_index,
							input.output_index
						],
						function( rows )
						{
							if ( rows.length > 1 )
							{
								throw Error( "more than 1 src output" );
							}
							if ( rows.length === 0 )
							{
								return cb( "input unit " + input.unit + " not found" );
							}

							var src_output	= rows[ 0 ];
							if ( typeof src_output.amount !== 'number' )
							{
								throw Error( "src output amount is not a number" );
							}
							if ( ! ( ! payload.asset && ! src_output.asset || payload.asset === src_output.asset ) )
							{
								return cb( "asset mismatch" );
							}

							//if (src_output.is_stable !== 1) // we allow immediate spends, that's why the error is transient
							//    return cb(_validation_utils.createTransientError("input unit is not on stable MC yet, unit "+objUnit.unit+", input "+input.unit));

							if ( src_output.main_chain_index !== null &&
								src_output.main_chain_index <= objValidationState.last_ball_mci &&
								src_output.sequence !== 'good' )
							{
								return cb( "stable input unit " + input.unit + " is not serial" );
							}
							if ( objValidationState.last_ball_mci < _constants.spendUnconfirmedUpgradeMci )
							{
								if ( ! objAsset ||
									! objAsset.is_private )
								{
									//	for public payments, you can't spend unconfirmed transactions
									if ( src_output.main_chain_index > objValidationState.last_ball_mci ||
										src_output.main_chain_index === null )
									{
										return cb( "src output must be before last ball" );
									}
								}
								if ( src_output.sequence !== 'good' )
								{
									//	it is also stable or private
									return cb( "input unit " + input.unit + " is not serial" );
								}
							}
							else
							{
								//
								//	after this MCI,
								//	spending unconfirmed is allowed for public assets too,
								//	non-good sequence will be inherited
								//
								if ( src_output.sequence !== 'good' )
								{
									if ( objValidationState.sequence === 'good' ||
										objValidationState.sequence === 'temp-bad' )
									{
										objValidationState.sequence = src_output.sequence;
									}
								}
							}

							//	...
							var owner_address	= src_output.address;
							if ( arrAuthorAddresses.indexOf( owner_address ) === -1 )
							{
								return cb( "output owner is not among authors" );
							}
							if ( denomination !== src_output.denomination )
							{
								return cb( "denomination mismatch" );
							}
							if ( objAsset && objAsset.auto_destroy &&
								owner_address === objAsset.definer_address )
							{
								return cb( "this output was destroyed by sending it to definer address" );
							}
							if ( objAsset &&
								objAsset.spender_attested &&
								objAsset.arrAttestedAddresses.indexOf( owner_address ) === -1 )
							{
								return cb( "owner address is not attested" );
							}

							if ( arrInputAddresses.indexOf( owner_address ) === -1 )
							{
								arrInputAddresses.push( owner_address );
							}

							//	...
							total_input += src_output.amount;

							if ( ! objAsset || ! objAsset.is_private )
							{
								return checkInputDoubleSpend( cb );
							}

							//
							//	for private payments only, unit already saved (if public, we are already before last ball)
							//	when divisible, the asset is also non-transferrable and auto-destroy,
							//	then this transfer is a transfer back to the issuer
							//	and input.unit is known both to payer and the payee (issuer), even if light
							//
							_graph.determineIfIncluded
							(
								conn,
								input.unit,
								[
									objUnit.unit
								],
								function( bIncluded )
								{
									if ( ! bIncluded )
									{
										return cb( "input " + input.unit + " is not in your genes" );
									}

									//	...
									checkInputDoubleSpend( cb );
								}
							);
						}
					);
					break;

				case "headers_commission":
				case "witnessing":
					if ( type === "headers_commission" )
					{
						if ( bHaveWitnessings )
						{
							return cb( "all headers commissions must come before witnessings" );
						}

						//	...
						bHaveHeadersComissions = true;
					}
					else
					{
						bHaveWitnessings = true;
					}

					if ( objAsset )
					{
						return cb( "only base asset can have " + type );
					}
					if ( _validation_utils.hasFieldsExcept( input, [ "type", "from_main_chain_index", "to_main_chain_index", "address" ] ) )
					{
						return cb( "unknown fields in witnessing input" );
					}
					if ( ! _validation_utils.isNonnegativeInteger( input.from_main_chain_index ) )
					{
						return cb( "from_main_chain_index must be nonnegative int" );
					}
					if ( ! _validation_utils.isNonnegativeInteger( input.to_main_chain_index ) )
					{
						return cb( "to_main_chain_index must be nonnegative int" );
					}
					if ( input.from_main_chain_index > input.to_main_chain_index )
					{
						return cb( "from_main_chain_index > input.to_main_chain_index" );
					}
					if ( input.to_main_chain_index > objValidationState.last_ball_mci )
					{
						return cb( "to_main_chain_index > last_ball_mci" );
					}
					if ( input.from_main_chain_index > objValidationState.last_ball_mci )
					{
						return cb( "from_main_chain_index > last_ball_mci" );
					}

					//	...
					address	= null;

					//	...
					if ( arrAuthorAddresses.length === 1 )
					{
						if ( "address" in input )
						{
							return cb( "when single-authored, must not put address in " + type + " input" );
						}

						//	...
						address	= arrAuthorAddresses[ 0 ];
					}
					else
					{
						if ( typeof input.address !== "string" )
						{
							return cb( "when multi-authored, must put address in " + type + " input" );
						}
						if ( arrAuthorAddresses.indexOf( input.address ) === -1 )
						{
							return cb( type + " input address " + input.address + " is not an author" );
						}

						//	...
						address = input.address;
					}

					//	...
					input_key	= type + "-" + address + "-" + input.from_main_chain_index;

					//	...
					if ( objValidationState.arrInputKeys.indexOf( input_key ) >= 0 )
					{
						return cb( "input " + input_key + " already used" );
					}

					objValidationState.arrInputKeys.push( input_key );

					doubleSpendWhere	= "type=? AND from_main_chain_index=? AND address=? AND asset IS NULL";
					doubleSpendVars		= [ type, input.from_main_chain_index, address ];

					_mc_outputs.readNextSpendableMcIndex
					(
						conn,
						type,
						address,
						objValidationState.arrConflictingUnits,
						function( next_spendable_mc_index )
						{
							var max_mci;
							var calcFunc;

							if ( input.from_main_chain_index < next_spendable_mc_index )
							{
								//	gaps allowed, in case a unit becomes bad due to another address being nonserial
								return cb( type + " ranges must not overlap" );
							}

							//	...
							max_mci = ( type === "headers_commission" )
								? _headers_commission.getMaxSpendableMciForLastBallMci( objValidationState.last_ball_mci )
								: _paid_witnessing.getMaxSpendableMciForLastBallMci( objValidationState.last_ball_mci );

							if ( input.to_main_chain_index > max_mci )
							{
								return cb( type + " to_main_chain_index is too large" );
							}

							//	...
							calcFunc = ( type === "headers_commission" )
								? _mc_outputs.calcEarnings
								: _paid_witnessing.calcWitnessEarnings;

							calcFunc
							(
								conn,
								type,
								input.from_main_chain_index,
								input.to_main_chain_index,
								address,
								{
									ifError : function( err )
									{
										throw Error( err );
									},
									ifOk : function( commission )
									{
										if ( commission === 0 )
										{
											return cb( "zero " + type + " commission" );
										}

										//	...
										total_input += commission;
										checkInputDoubleSpend( cb );
									}
								}
							);
						}
					);
					break;

				default:
					return cb( "unrecognized input type: " + input.type );
			}
		},
		function( err )
		{
			_log.consoleLog( "inputs done " + payload.asset, arrInputAddresses, arrOutputAddresses );
			if ( err )
			{
				return callback( err );
			}

			if ( objAsset )
			{
				if ( total_input !== total_output )
				{
					return callback( "inputs and outputs do not balance: " + total_input + " !== " + total_output );
				}

				if ( ! objAsset.is_transferrable )
				{
					//	the condition holds for issues too
					if ( arrInputAddresses.length === 1 && arrInputAddresses[0] === objAsset.definer_address ||
						arrOutputAddresses.length === 1 && arrOutputAddresses[0] === objAsset.definer_address ||
						// sending payment to the definer and the change back to oneself
						! ( objAsset.fixed_denominations && objAsset.is_private )
						&& arrInputAddresses.length === 1 && arrOutputAddresses.length === 2
						&& arrOutputAddresses.indexOf( objAsset.definer_address ) >= 0
						&& arrOutputAddresses.indexOf( arrInputAddresses[ 0 ] ) >= 0 )
					{
						// good
					}
					else
					{
						return callback( "the asset is not transferable" );
					}
				}

				//	...
				_async.series
				(
					[
						function( cb )
						{
							if ( ! objAsset.spender_attested )
							{
								return cb();
							}

							//	...
							_storage.filterAttestedAddresses
							(
								conn,
								objAsset,
								objValidationState.last_ball_mci,
								arrOutputAddresses,
								function( arrAttestedOutputAddresses )
								{
									if ( arrAttestedOutputAddresses.length !== arrOutputAddresses.length )
									{
										return cb( "some output addresses are not attested" );
									}

									//	...
									cb();
								}
							);
						},
						function( cb )
						{
							var arrCondition;

							//	...
							arrCondition = bIssue
								? objAsset.issue_condition
								: objAsset.transfer_condition;

							if ( ! arrCondition )
							{
								return cb();
							}

							//	...
							_definition.evaluateAssetCondition
							(
								conn,
								payload.asset,
								arrCondition,
								objUnit,
								objValidationState,
								function( cond_err, bSatisfiesCondition )
								{
									if ( cond_err )
									{
										return cb( cond_err );
									}
									if ( ! bSatisfiesCondition )
									{
										return cb( "transfer or issue condition not satisfied" );
									}

									//	...
									_log.consoleLog( "_validatePaymentInputsAndOutputs with transfer/issue conditions done" );

									//	...
									cb();
								}
							);
						}
					],
					callback
				);
			}
			else
			{
				//	base asset
				if ( total_input !== total_output + objUnit.headers_commission + objUnit.payload_commission )
				{
					return callback
					(
						"inputs and outputs do not balance: " + total_input + " !== " + total_output + " + " + objUnit.headers_commission + " + " + objUnit.payload_commission
					);
				}

				//	...
				callback();
			}

			//	_log.consoleLog("_validatePaymentInputsAndOutputs done");
			//	if (objAsset)
			//		profiler2.stop('validate IO');
			//	callback();
		}
	);
}








/**
 *	exports
 */
exports.validatePayment		= validatePayment;