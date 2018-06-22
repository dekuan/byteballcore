/*jslint node: true */
"use strict";

var _async		= require( 'async' );
var _storage		= require( './storage.js' );
var _my_witnesses	= require( './my_witnesses.js' );
var _object_hash	= require( './object_hash.js' );
var _db			= require( './db.js' );
var _constants		= require( './constants.js' );
var _validation		= require( './validation.js' );



function prepareWitnessProof( arrWitnesses, last_stable_mci, handleResult )
{
	var arrWitnessChangeAndDefinitionJoints	= [];
	var arrUnstableMcJoints			= [];

	//	last ball units referenced from MC-majority-witnessed unstable MC units
	var arrLastBallUnits	= [];
	var last_ball_unit	= null;
	var last_ball_mci	= null;

	//	...
	_async.series
	(
		[
			function( cb )
			{
				_storage.determineIfWitnessAddressDefinitionsHaveReferences
				(
					_db,
					arrWitnesses,
					function( bWithReferences )
					{
						bWithReferences
							? cb( "some witnesses have references in their addresses, please change your witness list" )
							: cb();
					}
				);
			},
			function( cb )
			{
				//
				//	collect all unstable MC units
				//
				var arrFoundWitnesses	= [];

				//	...
				_db.query
				(
					"SELECT unit FROM units \
					WHERE is_on_main_chain = 1 AND is_stable = 0 \
					ORDER BY main_chain_index DESC",
					function( rows )
					{
						_async.eachSeries
						(
							rows,
							function( row, cb2 )
							{
								_storage.readJointWithBall( _db, row.unit, function( objJoint )
								{
									//
									//	the unit might get stabilized while we were reading other units
									//
									delete objJoint.ball;
									arrUnstableMcJoints.push( objJoint );

									for ( var i = 0; i < objJoint.unit.authors.length; i++ )
									{
										var address	= objJoint.unit.authors[ i ].address;
										if ( arrWitnesses.indexOf( address ) >= 0 &&
											arrFoundWitnesses.indexOf( address ) === -1 )
										{
											arrFoundWitnesses.push( address );
										}
									}

									//
									//	collect last balls of majority witnessed units
									//	(genesis lacks last_ball_unit)
									//
									if ( objJoint.unit.last_ball_unit &&
										arrFoundWitnesses.length >= _constants.MAJORITY_OF_WITNESSES )
									{
										arrLastBallUnits.push( objJoint.unit.last_ball_unit );
									}

									//	...
									cb2();
								} );
							},
							cb
						);
					}
				);
			},
			function( cb )
			{
				//	select the newest last ball unit
				if ( arrLastBallUnits.length === 0 )
					return cb( "your witness list might be too much off, too few witness authored units" );

				//	...
				_db.query
				(
					"SELECT unit, main_chain_index FROM units WHERE unit IN(?) ORDER BY main_chain_index DESC LIMIT 1",
					[
						arrLastBallUnits
					],
					function( rows )
					{
						last_ball_unit	= rows[ 0 ].unit;
						last_ball_mci	= rows[ 0 ].main_chain_index;
						( last_stable_mci >= last_ball_mci )
							? cb( "already_current" )
							: cb();
					}
				);
			},
			function( cb )
			{
				//	add definition changes and new definitions of witnesses
				var after_last_stable_mci_cond	= ( last_stable_mci > 0 )
					? "latest_included_mc_index>=" + last_stable_mci
					: "1";

				//	...
				_db.query
				(
					/*"SELECT DISTINCT units.unit \n\
					FROM unit_authors \n\
					JOIN units USING(unit) \n\
					LEFT JOIN address_definition_changes \n\
						ON units.unit=address_definition_changes.unit AND unit_authors.address=address_definition_changes.address \n\
					WHERE unit_authors.address IN(?) AND "+after_last_stable_mci_cond+" AND is_stable=1 AND sequence='good' \n\
						AND (unit_authors.definition_chash IS NOT NULL OR address_definition_changes.unit IS NOT NULL) \n\
					ORDER BY `level`",
					[arrWitnesses],*/
					"SELECT unit, `level` \n\
					FROM unit_authors "+_db.forceIndex('unitAuthorsIndexByAddressDefinitionChash')+" \n\
					CROSS JOIN units USING(unit) \n\
					WHERE address IN(?) AND definition_chash IS NOT NULL AND "+after_last_stable_mci_cond+" AND is_stable=1 AND sequence='good' \n\
					UNION \n\
					SELECT unit, `level` \n\
					FROM address_definition_changes \n\
					CROSS JOIN units USING(unit) \n\
					WHERE address_definition_changes.address IN(?) AND "+after_last_stable_mci_cond+" AND is_stable=1 AND sequence='good' \n\
					ORDER BY `level`",
					[
						arrWitnesses,
						arrWitnesses
					],
					function( rows )
					{
						_async.eachSeries
						(
							rows,
							function( row, cb2 )
							{
								_storage.readJoint
								(
									_db,
									row.unit,
									{
										ifNotFound : function()
										{
											throw Error( "prepareWitnessProof definition changes: not found " + row.unit );
										},
										ifFound : function( objJoint )
										{
											arrWitnessChangeAndDefinitionJoints.push( objJoint );
											cb2();
										}
									}
								);
							},
							cb
						);
					}
				);
			}
		],
		function( err )
		{
			if ( err )
			{
				return handleResult( err );
			}

			//	...
			handleResult
			(
				null,
				arrUnstableMcJoints,
				arrWitnessChangeAndDefinitionJoints,
				last_ball_unit,
				last_ball_mci
			);
		}
	);
}


function processWitnessProof( arrUnstableMcJoints, arrWitnessChangeAndDefinitionJoints, bFromCurrent, handleResult )
{
	_my_witnesses.readMyWitnesses
	(
		function( arrWitnesses )
		{
			//	unstable MC joints
			var arrParentUnits		= null;
			var arrFoundWitnesses		= [];
			var arrLastBallUnits		= [];
			var assocLastBallByLastBallUnit	= {};
			var arrWitnessJoints		= [];

			for ( var i = 0; i < arrUnstableMcJoints.length; i++ )
			{
				var objJoint	= arrUnstableMcJoints[ i ];
				var objUnit	= objJoint.unit;
				if ( objJoint.ball )
				{
					return handleResult( "unstable mc but has ball" );
				}
				if ( ! _validation.hasValidHashes( objJoint ) )
				{
					return handleResult( "invalid hash" );
				}
				if ( arrParentUnits && arrParentUnits.indexOf( objUnit.unit ) === -1 )
				{
					return handleResult( "not in parents" );
				}

				var bAddedJoint	= false;
				for ( var j = 0; j < objUnit.authors.length; j++ )
				{
					var address = objUnit.authors[j].address;
					if ( arrWitnesses.indexOf( address ) >= 0 )
					{
						if ( arrFoundWitnesses.indexOf( address ) === -1 )
						{
							arrFoundWitnesses.push( address );
						}
						if ( ! bAddedJoint )
						{
							arrWitnessJoints.push( objJoint );
						}

						//	...
						bAddedJoint = true;
					}
				}

				//	...
				arrParentUnits = objUnit.parent_units;
				if ( objUnit.last_ball_unit &&
					arrFoundWitnesses.length >= _constants.MAJORITY_OF_WITNESSES )
				{
					arrLastBallUnits.push( objUnit.last_ball_unit );
					assocLastBallByLastBallUnit[ objUnit.last_ball_unit ] = objUnit.last_ball;
				}
			}

			if ( arrFoundWitnesses.length < _constants.MAJORITY_OF_WITNESSES )
			{
				return handleResult( "not enough witnesses" );
			}

			if ( arrLastBallUnits.length === 0 )
			{
				throw Error( "processWitnessProof: no last ball units" );
			}

			//	changes and definitions of witnesses
			for ( var i = 0; i < arrWitnessChangeAndDefinitionJoints.length; i++ )
			{
				var objJoint	= arrWitnessChangeAndDefinitionJoints[ i ];
				var objUnit	= objJoint.unit;

				if ( ! objJoint.ball )
				{
					return handleResult( "witness_change_and_definition_joints: joint without ball" );
				}
				if ( ! _validation.hasValidHashes( objJoint ) )
				{
					return handleResult( "witness_change_and_definition_joints: invalid hash" );
				}

				var bAuthoredByWitness = false;
				for ( var j = 0; j < objUnit.authors.length; j ++ )
				{
					var address	= objUnit.authors[ j ].address;
					if ( arrWitnesses.indexOf( address ) >= 0 )
					{
						bAuthoredByWitness = true;
					}
				}

				if ( ! bAuthoredByWitness )
				{
					return handleResult( "not authored by my witness" );
				}
			}

			//	...
			var assocDefinitions		= {};	//	keyed by definition chash
			var assocDefinitionChashes	= {};	//	keyed by address

			//	checks signatures and updates definitions
			function validateUnit( objUnit, bRequireDefinitionOrChange, cb2 )
			{
				var bFound	= false;

				//	...
				_async.eachSeries
				(
					objUnit.authors,
					function( author, cb3 )
					{
						var address	= author.address;
						if ( arrWitnesses.indexOf( address ) === -1 )
						{
							//	not a witness - skip it
							return cb3();
						}

						var definition_chash	= assocDefinitionChashes[ address ];
						if ( ! definition_chash )
						{
							throw Error( "definition chash not known for address " + address );
						}
						if ( author.definition )
						{
							if ( _object_hash.getChash160( author.definition ) !== definition_chash )
							{
								return cb3( "definition doesn't hash to the expected value" );
							}

							//	...
							assocDefinitions[ definition_chash ] = author.definition;
							bFound = true;
						}

						function handleAuthor()
						{
							//	FIX
							_validation.validateAuthorSignaturesWithoutReferences
							(
								author,
								objUnit,
								assocDefinitions[ definition_chash ],
								function( err )
								{
									if ( err )
										return cb3( err );

									for ( var i = 0; i < objUnit.messages.length; i ++ )
									{
										var message	= objUnit.messages[ i ];

										if ( message.app === 'address_definition_change' &&
											( message.payload.address === address ||
												objUnit.authors.length === 1 &&
												objUnit.authors[ 0 ].address === address ) )
										{
											assocDefinitionChashes[ address ] = message.payload.definition_chash;
											bFound = true;
										}
									}

									//	....
									cb3();
								}
							);
						}

						if ( assocDefinitions[ definition_chash ] )
						{
							return handleAuthor();
						}

						//	...
						_storage.readDefinition
						(
							_db,
							definition_chash,
							{
								ifFound : function( arrDefinition )
								{
									assocDefinitions[ definition_chash ] = arrDefinition;
									handleAuthor();
								},
								ifDefinitionNotFound : function( d )
								{
									throw Error( "definition " + definition_chash + " not found, address " + address );
								}
							}
						);
					},
					function( err )
					{
						if ( err )
						{
							return cb2( err );
						}
						if ( bRequireDefinitionOrChange && ! bFound )
						{
							return cb2( "neither definition nor change" );
						}

						//	...
						cb2();
					}
				); // each authors
			}

			//	...
			var unlock = null;

			//	...
			_async.series
			(
				[
					function( cb )
					{
						//	read latest known definitions of witness addresses
						if ( ! bFromCurrent )
						{
							arrWitnesses.forEach
							(
								function( address )
								{
									assocDefinitionChashes[ address ] = address;
								}
							);

							//	...
							return cb();
						}

						//	...
						_async.eachSeries
						(
							arrWitnesses,
							function( address, cb2 )
							{
								_storage.readDefinitionByAddress
								(
									_db,
									address,
									null,
									{
										ifFound : function( arrDefinition )
										{
											var definition_chash = _object_hash.getChash160( arrDefinition );
											assocDefinitions[ definition_chash ]	= arrDefinition;
											assocDefinitionChashes[ address ]	= definition_chash;

											//	...
											cb2();
										},
										ifDefinitionNotFound : function( definition_chash )
										{
											assocDefinitionChashes[ address ]	= definition_chash;

											//	...
											cb2();
										}
									}
								);
							},
							cb
						);
					},
					function( cb )
					{
						//	handle changes of definitions
						_async.eachSeries
						(
							arrWitnessChangeAndDefinitionJoints,
							function( objJoint, cb2 )
							{
								var objUnit	= objJoint.unit;

								if ( ! bFromCurrent )
								{
									return validateUnit( objUnit, true, cb2 );
								}

								//	...
								_db.query
								(
									"SELECT 1 FROM units WHERE unit=? AND is_stable=1",
									[
										objUnit.unit
									],
									function( rows )
									{
										if ( rows.length > 0 )
										{
											//	already known and stable - skip it
											return cb2();
										}

										//	...
										validateUnit( objUnit, true, cb2 );
									}
								);
							},
							cb
						);	// each change or definition
					},
					function( cb )
					{
						//	check signatures of unstable witness joints
						_async.eachSeries
						(
							arrWitnessJoints.reverse(),	//	they came in reverse chronological order, reverse() reverses in place
							function( objJoint, cb2 )
							{
								validateUnit( objJoint.unit, false, cb2 );
							},
							cb
						);
					},
				],
				function( err )
				{
					err
						? handleResult( err )
						: handleResult( null, arrLastBallUnits, assocLastBallByLastBallUnit );
				}
			);
		}
	);
}




/**
 *	exports
 */
exports.prepareWitnessProof = prepareWitnessProof;
exports.processWitnessProof = processWitnessProof;
