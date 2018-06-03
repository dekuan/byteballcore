/*jslint node: true */
"use strict";

var _async			= require( 'async' );
var _log			= require( './log.js' );
var _storage			= require( './storage.js' );
var _graph			= require( './graph.js' );
var _object_hash		= require( './object_hash.js' );
var _chash			= require( './chash.js' );
var _constants			= require( './constants.js' );
var _definition			= require( './definition.js' );
var _breadcrumbs		= require( './breadcrumbs.js' );

var _validation_utils		= require( './validation_utils.js' );




/**
 *	CValidateAuthors
 *
 *	@param conn_
 *	@param arrAuthors_
 *	@param objUnit_
 *	@param objValidationState_
 *	@param callback_
 *	@returns {*}
 *	@constructor
 */
function CValidateAuthors( conn_, arrAuthors_, objUnit_, objValidationState_, callback_ )
{
	//	...
	var m_sPrevAddress	= "";
	var m_bNonSerial	= false;


	/**
	 *	handle
	 *	@returns {*}
	 */
	this.handle = function()
	{
		var i;
		var objAuthor;

		if ( arrAuthors_.length > _constants.MAX_AUTHORS_PER_UNIT )
		{
			//
			//	this is anti-spam.
			//	Otherwise an attacker would send nonserial balls signed by zillions of authors.
			//
			return callback_( "too many authors" );
		}

		for ( i = 0; i < arrAuthors_.length; i++ )
		{
			objAuthor	= arrAuthors_[ i ];
			if ( objAuthor.address <= m_sPrevAddress )
			{
				return callback_( "author addresses not sorted" );
			}

			//	...
			m_sPrevAddress	= objAuthor.address;
		}

		//
		//	...
		//
		objValidationState_.arrAddressesWithForkedPath	= [];
		objValidationState_.unit_hash_to_sign		= _object_hash.getUnitHashToSign( objUnit_ );

		//
		//	...
		//
		_async.eachSeries
		(
			arrAuthors_,
			function( objAuthorItem, pfnCallback )
			{
				_validateAuthor( objAuthorItem, pfnCallback );
			},
			callback_
		);
	};



	//	--------------------------------------------------------------------------------
	//	Private
	//	--------------------------------------------------------------------------------
	function _constructor()
	{
	}


	function _validateAuthor( objAuthor, callback )
	{
		var sPath;
		var arrAddressDefinition;

		if ( ! _validation_utils.isStringOfLength( objAuthor.address, 32 ) )
		{
			return callback( "wrong address length" );
		}
		if ( _validation_utils.hasFieldsExcept( objAuthor, [ "address", "authentifiers", "definition" ] ) )
		{
			return callback( "unknown fields in author" );
		}
		if ( ! _validation_utils.isNonemptyObject( objAuthor.authentifiers ) && ! objUnit_.content_hash )
		{
			return callback( "no authentifiers" );
		}

		//	...
		for ( sPath in objAuthor.authentifiers )
		{
			if ( ! objAuthor.authentifiers.hasOwnProperty( sPath ) )
			{
				continue;
			}

			//	...
			if ( ! _validation_utils.isNonemptyString( objAuthor.authentifiers[ sPath ] ) )
			{
				return callback( "authentifiers must be nonempty strings" );
			}
			if ( objAuthor.authentifiers[ sPath ].length > _constants.MAX_AUTHENTIFIER_LENGTH )
			{
				return callback( "authentifier too long" );
			}
		}

		//	...
		m_bNonSerial		= false;
		arrAddressDefinition	= objAuthor.definition;

		//	...
		if ( _validation_utils.isNonemptyArray( arrAddressDefinition ) )
		{
			//
			//	todo: check that the address is really new?
			//
			_validateAuthentifiers( objAuthor, callback, arrAddressDefinition );
		}
		else if ( ! ( "definition" in objAuthor ) )
		{
			if ( ! _chash.isChashValid( objAuthor.address ) )
			{
				return callback( "address checksum invalid" );
			}
			if ( objUnit_.content_hash )
			{
				//	nothing else to check
				objValidationState_.sequence = 'final-bad';
				return callback();
			}

			//	we check signatures using the latest address definition before last ball
			_storage.readDefinitionByAddress
			(
				conn_,
				objAuthor.address,
				objValidationState_.last_ball_mci,
				{
					ifDefinitionNotFound : function( definition_chash )
					{
						callback( "definition " + definition_chash + " bound to address " + objAuthor.address + " is not defined" );
					},
					ifFound : function( arrAddressDefinition )
					{
						//
						//	...
						//
						_validateAuthentifiers( objAuthor, callback, arrAddressDefinition );
					}
				}
			);
		}
		else
		{
			return callback( "bad type of definition" );
		}
	}


	//	...
	function _validateAuthentifiers( objAuthor, callback, arrAddressDefinition )
	{
		_definition.validateAuthentifiers
		(
			conn_,
			objAuthor.address,
			null,
			arrAddressDefinition,
			objUnit_,
			objValidationState_,
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
				_checkSerialAddressUse( objAuthor, callback );
			}
		);
	}

	function _findConflictingUnits( objAuthor, callback, handleConflictingUnits )
	{
		//
		//	var cross = (objValidationState_.max_known_mci - objValidationState_.max_parent_limci < 1000) ? 'CROSS' : '';
		//

		conn_.query
		(
			// _left_ join forces use of indexes in units
			/*
			"SELECT unit, is_stable \n\
			FROM units \n\
			"+cross+" JOIN unit_authors USING(unit) \n\
			WHERE address=? AND (main_chain_index>? OR main_chain_index IS NULL) AND unit != ?",
			[objAuthor.address, objValidationState_.max_parent_limci, objUnit_.unit],*/
			"SELECT unit, is_stable \n\
			FROM unit_authors \n\
			CROSS JOIN units USING(unit) \n\
			WHERE address=? AND _mci>? AND unit != ? \n\
			UNION \n\
			SELECT unit, is_stable \n\
			FROM unit_authors \n\
			CROSS JOIN units USING(unit) \n\
			WHERE address=? AND _mci IS NULL AND unit != ?",
			[
				objAuthor.address,
				objValidationState_.max_parent_limci,
				objUnit_.unit,
				objAuthor.address,
				objUnit_.unit
			],
			function( rows )
			{
				var arrConflictingUnitProps = [];

				//	...
				_async.eachSeries
				(
					rows,
					function( row, cb )
					{
						_graph.determineIfIncludedOrEqual
						(
							conn_,
							row.unit,
							objUnit_.parent_units,
							function( bIncluded )
							{
								if ( ! bIncluded )
								{
									arrConflictingUnitProps.push( row );
								}

								//	...
								cb();
							}
						);
					},
					function()
					{
						handleConflictingUnits( arrConflictingUnitProps );
					}
				);
			}
		);
	}

	function _checkSerialAddressUse( objAuthor, callback )
	{
		//	...
		_findConflictingUnits
		(
			objAuthor,
			callback,
			function( arrConflictingUnitProps )
			{
				var arrConflictingUnits;
				var bConflictsWithStableUnits;
				var arrUnstableConflictingUnits;

				if ( arrConflictingUnitProps.length === 0 )
				{
					//
					//	no conflicting units
					//	we can have 2 authors.
					//	If the 1st author gave bad sequence but the 2nd is good then don't overwrite
					//
					objValidationState_.sequence	= objValidationState_.sequence || 'good';
					return _checkNoPendingChangeOfDefinitionChash( objAuthor, callback );
				}

				//	...
				arrConflictingUnits = arrConflictingUnitProps.map
				(
					function( objConflictingUnitProps )
					{
						return objConflictingUnitProps.unit;
					}
				);

				//	...
				_breadcrumbs.add( "========== found conflicting units " + arrConflictingUnits + " =========" );
				_breadcrumbs.add( "========== will accept a conflicting unit " + objUnit_.unit + " =========" );

				objValidationState_.arrAddressesWithForkedPath.push( objAuthor.address );
				objValidationState_.arrConflictingUnits	= ( objValidationState_.arrConflictingUnits || [] ).concat( arrConflictingUnits );

				//	...
				m_bNonSerial = true;
				var arrUnstableConflictingUnitProps = arrConflictingUnitProps.filter
				(
					function( objConflictingUnitProps )
					{
						return ( objConflictingUnitProps.is_stable === 0 );
					}
				);

				//	...
				bConflictsWithStableUnits = arrConflictingUnitProps.some
				(
					function( objConflictingUnitProps )
					{
						return ( objConflictingUnitProps.is_stable === 1 );
					}
				);

				if ( objValidationState_.sequence !== 'final-bad' )
				{
					//	if it were already final-bad because of 1st author, it can't become temp-bad due to 2nd author
					objValidationState_.sequence = bConflictsWithStableUnits ? 'final-bad' : 'temp-bad';
				}

				//	...
				arrUnstableConflictingUnits = arrUnstableConflictingUnitProps.map
				(
					function( objConflictingUnitProps )
					{
						return objConflictingUnitProps.unit;
					}
				);

				if ( bConflictsWithStableUnits )
				{
					//	don't temp-bad the unstable conflicting units
					return _checkNoPendingChangeOfDefinitionChash( objAuthor, callback );
				}
				if ( arrUnstableConflictingUnits.length === 0 )
				{
					return _checkNoPendingChangeOfDefinitionChash( objAuthor, callback );
				}

				//	we don't modify the _db during validation, schedule the update for the write
				objValidationState_.arrAdditionalQueries.push
				(
					{
						sql	: "UPDATE units SET sequence='temp-bad' WHERE unit IN(?) AND +sequence='good'",
						params	: [ arrUnstableConflictingUnits ]
					}
				);

				//	...
				_checkNoPendingChangeOfDefinitionChash( objAuthor, callback );
			}
		);
	}

	//
	//	don't allow contradicting pending keychanges.
	//	We don't trust pending keychanges even when they are serial,
	//		as another unit may arrive and make them nonserial
	//
	function _checkNoPendingChangeOfDefinitionChash( objAuthor, callback )
	{
		//	var filter = m_bNonSerial ? "AND sequence='good'" : "";
		conn_.query
		(
			"SELECT unit FROM address_definition_changes JOIN units USING(unit) \n\
			WHERE address=? AND (is_stable=0 OR main_chain_index>? OR main_chain_index IS NULL)",
			[
				objAuthor.address,
				objValidationState_.last_ball_mci
			],
			function( rows )
			{
				if ( rows.length === 0 )
				{
					return _checkNoPendingDefinition( objAuthor, callback );
				}
				if ( ! m_bNonSerial || objValidationState_.arrAddressesWithForkedPath.indexOf( objAuthor.address ) === -1 )
				{
					return callback( "you can't send anything before your last keychange is stable and before last ball" );
				}

				//	from this point, our unit is nonserial
				_async.eachSeries
				(
					rows,
					function( row, cb )
					{
						_graph.determineIfIncludedOrEqual
						(
							conn_,
							row.unit,
							objUnit_.parent_units,
							function( bIncluded )
							{
								if ( bIncluded )
								{
									_log.consoleLog
									(
										"_checkNoPendingChangeOfDefinitionChash: unit " + row.unit + " is included"
									);
								}

								//	...
								bIncluded ? cb( "found" ) : cb();
							}
						);
					},
					function( err )
					{
						( err === "found" )
							? callback( "you can't send anything before your last included keychange is stable and before last ball (self is nonserial)" )
							: _checkNoPendingDefinition( objAuthor, callback );
					}
				);
			}
		);
	}

	//
	//	We don't trust pending definitions even when they are serial, as another unit may arrive and make them nonserial,
	//	then the definition will be removed
	//
	function _checkNoPendingDefinition( objAuthor, callback )
	{
		//	var next = checkNoPendingOrRetrievableNonserialIncluded;

		//var filter = m_bNonSerial ? "AND sequence='good'" : "";
		//	var cross = (objValidationState_.max_known_mci - objValidationState_.last_ball_mci < 1000) ? 'CROSS' : '';
		conn_.query
		(
			// _left_ join forces use of indexes in units
			//	"SELECT unit FROM units "+cross+" JOIN unit_authors USING(unit) \n\
			//	WHERE address=? AND definition_chash IS NOT NULL AND ( /* is_stable=0 OR */ main_chain_index>? OR main_chain_index IS NULL)",
			//	[objAuthor.address, objValidationState_.last_ball_mci],
			"SELECT unit FROM unit_authors WHERE address=? AND definition_chash IS NOT NULL AND _mci>?  \n\
			UNION \n\
			SELECT unit FROM unit_authors WHERE address=? AND definition_chash IS NOT NULL AND _mci IS NULL",
			[
				objAuthor.address,
				objValidationState_.last_ball_mci,
				objAuthor.address
			],
			function( rows )
			{
				if ( rows.length === 0 )
				{
					return _validateDefinition( objAuthor, callback );
				}
				if ( ! m_bNonSerial || objValidationState_.arrAddressesWithForkedPath.indexOf( objAuthor.address ) === -1 )
				{
					return callback( "you can't send anything before your last definition is stable and before last ball" );
				}

				//	from this point, our unit is nonserial
				_async.eachSeries
				(
					rows,
					function( row, cb )
					{
						_graph.determineIfIncludedOrEqual
						(
							conn_,
							row.unit,
							objUnit_.parent_units,
							function( bIncluded )
							{
								if ( bIncluded )
								{
									_log.consoleLog( "_checkNoPendingDefinition: unit " + row.unit + " is included" );
								}

								//	...
								bIncluded ? cb( "found" ) : cb();
							}
						);
					},
					function( err )
					{
						( err === "found" )
							? callback( "you can't send anything before your last included definition is stable and before last ball (self is nonserial)" )
							: _validateDefinition( objAuthor, callback );
					}
				);
			}
		);
	}

	//
	//	This was bad idea.  An uncovered nonserial, if not archived, will block new units from this address forever.
	//
	// function checkNoPendingOrRetrievableNonserialIncluded(){
	// 	conn_.query(
	// 		"SELECT lb_units.main_chain_index FROM units JOIN units AS lb_units ON units.last_ball_unit=lb_units.unit \n\
	// 		WHERE units.is_on_main_chain=1 AND units.main_chain_index=?",
	// 		[objValidationState_.last_ball_mci],
	// 		function(lb_rows){
	// 			var last_ball_of_last_ball_mci = (lb_rows.length > 0) ? lb_rows[0].main_chain_index : 0;
	// 			conn_.query(
	// 				"SELECT unit FROM unit_authors JOIN units USING(unit) \n\
	// 				WHERE address=? AND (is_stable=0 OR main_chain_index>?) AND sequence!='good'",
	// 				[objAuthor.address, last_ball_of_last_ball_mci],
	// 				function(rows){
	// 					if (rows.length === 0)
	// 						return _validateDefinition( objAuthor, callback );
	// 					if (!m_bNonSerial)
	// 						return callback("you can't send anything before all your nonserial units are stable and before last ball of last ball");
	// 					// from this point, the unit is nonserial
	// 					_async.eachSeries(
	// 						rows,
	// 						function(row, cb){
	// 							_graph.determineIfIncludedOrEqual(conn_, row.unit, objUnit_.parent_units, function(bIncluded){
	// 								if (bIncluded)
	// 									_log.consoleLog("checkNoPendingOrRetrievableNonserialIncluded: unit "+row.unit+" is included");
	// 								bIncluded ? cb("found") : cb();
	// 							});
	// 						},
	// 						function(err){
	// 							(err === "found")
	// 								? callback("you can't send anything before all your included nonserial units are stable \
	// 										   and lie before last ball of last ball (self is nonserial)")
	// 								: _validateDefinition( objAuthor, callback );
	// 						}
	// 					);
	// 				}
	// 			);
	// 		}
	// 	);
	// }

	function _validateDefinition( objAuthor, callback )
	{
		var arrAddressDefinition;

		if ( ! ( "definition" in objAuthor ) )
		{
			return callback();
		}

		//	the rest assumes that the definition is explicitly defined
		arrAddressDefinition = objAuthor.definition;

		//
		//	...
		//
		_storage.readDefinitionByAddress
		(
			conn_,
			objAuthor.address,
			objValidationState_.last_ball_mci,
			{
				ifDefinitionNotFound : function( definition_chash )
				{
					//	first use of the definition_chash
					//	(in particular, of the address, when definition_chash=address)
					if ( _object_hash.getChash160( arrAddressDefinition ) !== definition_chash )
					{
						return callback
						(
							"wrong definition: " + _object_hash.getChash160( arrAddressDefinition ) + "!==" + definition_chash
						);
					}

					//	...
					callback();
				},
				ifFound : function( arrAddressDefinition2 )
				{
					//	arrAddressDefinition2 can be different
					_handleDuplicateAddressDefinition( objAuthor, callback, arrAddressDefinition2 );
				}
			}
		);
	}

	function _handleDuplicateAddressDefinition( objAuthor, callback, arrAddressDefinition )
	{
		if ( ! m_bNonSerial ||
			objValidationState_.arrAddressesWithForkedPath.indexOf( objAuthor.address ) === -1 )
		{
			return callback( "duplicate definition of address " + objAuthor.address + ", m_bNonSerial=" + m_bNonSerial );
		}

		//
		//	todo: investigate if this can split the nodes
		//	in one particular case,
		//	the attacker changes his definition then quickly sends a new ball with the old definition
		//	- the new definition will not be active yet
		//
		if ( _object_hash.getChash160( arrAddressDefinition ) !== _object_hash.getChash160( objAuthor.definition ) )
		{
			return callback( "unit definition doesn't match the stored definition" );
		}

		//
		//	let it be for now. Eventually,
		//	at most one of the balls will be declared good
		//
		callback();
	}



	//
	//	...
	//
	_constructor();
}





/**
 *	exports
 */
exports.CValidateAuthors	= CValidateAuthors;
