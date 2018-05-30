/*jslint node: true */
"use strict";

let _			= require('lodash');
let async		= require('async');
let log			= require( './log.js' );
let constants		= require("./constants.js");
let conf		= require("./conf.js");
let storage		= require('./storage.js');
let db			= require('./db.js');
let objectHash		= require("./object_hash.js");
let mutex		= require('./mutex.js');
let main_chain		= require("./main_chain.js");
let Definition		= require("./definition.js");
let eventBus		= require('./event_bus.js');
let profilerex		= require('./profilerex.js');

let count_writes			= 0;
let count_units_in_prev_analyze		= 0;

let start_time				= 0;
let prev_time				= 0;




/**
 *	save joint
 */
function saveJoint( objJoint, objValidationState, preCommitCallback, onDone )
{
	let objUnit	= objJoint.unit;
	log.consoleLog( "\nsaving unit " + objUnit.unit );
	//#profiler.start();

	//
	//	...
	//
	db.takeConnectionFromPool
	(
		function( conn )
		{
			let arrQueries = [];

			//	...
			conn.addQuery
			(
				arrQueries,
				"BEGIN"
			);

			//
			//	additional queries generated by the validator,
			//	used only when received a doublespend
			//
			for ( let i = 0; i < objValidationState.arrAdditionalQueries.length; i++ )
			{
				let objAdditionalQuery	= objValidationState.arrAdditionalQueries[ i ];
				log.consoleLog( "----- applying additional queries: " + objAdditionalQuery.sql );
				conn.addQuery
				(
					arrQueries,
					objAdditionalQuery.sql,
					objAdditionalQuery.params
				);
			}


			//
			//	Add new unit
			//
			let fields = "unit, version, alt, witness_list_unit, last_ball_unit, headers_commission, payload_commission, sequence, content_hash";
			let values = "?,?,?,?,?,?,?,?,?";
			let params =
				[
					objUnit.unit,
					objUnit.version,
					objUnit.alt,
					objUnit.witness_list_unit,
					objUnit.last_ball_unit,
					objUnit.headers_commission || 0,
					objUnit.payload_commission || 0,
					objValidationState.sequence,
					objUnit.content_hash
				];
			if ( conf.bLight )
			{
				fields += ", main_chain_index, creation_date";
				values += ",?," + conn.getFromUnixTime( "?" );
				params.push
				(
					objUnit.main_chain_index,
					objUnit.timestamp
				);
			}

			conn.addQuery
			(
				arrQueries,
				"INSERT INTO units (" + fields + ") VALUES (" + values + ")",
				params
			);


			//
			//	....
			//
			if ( objJoint.ball && ! conf.bLight )
			{
				conn.addQuery
				(
					arrQueries,
					"INSERT INTO balls (ball, unit) VALUES(?,?)",
					[
						objJoint.ball,
						objUnit.unit
					]
				);
				conn.addQuery
				(
					arrQueries,
					"DELETE FROM hash_tree_balls WHERE ball=? AND unit=?",
					[
						objJoint.ball,
						objUnit.unit
					]
				);

				if ( objJoint.skiplist_units )
				{
					for ( let i = 0; i < objJoint.skiplist_units.length; i ++ )
					{
						conn.addQuery
						(
							arrQueries,
							"INSERT INTO skiplist_units (unit, skiplist_unit) VALUES (?,?)",
							[
								objUnit.unit,
								objJoint.skiplist_units[ i ]
							]
						);
					}
				}
			}

			if ( objUnit.parent_units )
			{
				for ( let i = 0; i < objUnit.parent_units.length; i ++ )
				{
					conn.addQuery
					(
						arrQueries,
						"INSERT INTO parenthoods (child_unit, parent_unit) VALUES(?,?)",
						[
							objUnit.unit,
							objUnit.parent_units[ i ]
						]
					);
				}
			}

			let bGenesis	= storage.isGenesisUnit( objUnit.unit );
			if ( bGenesis )
			{
				conn.addQuery
				(
					arrQueries,
					"UPDATE units SET is_on_main_chain=1, main_chain_index=0, is_stable=1, level=0, witnessed_level=0 \n\
					WHERE unit=?",
					[
						objUnit.unit
					]
				);
			}
			else
			{
				conn.addQuery
				(
					arrQueries,
					"UPDATE units SET is_free=0 WHERE unit IN(?)",
					[
						objUnit.parent_units
					],
					function( result )
					{
						//	in sqlite3, result.affectedRows actually returns the number of _matched_ rows
						let count_consumed_free_units	= result.affectedRows;
						log.consoleLog( count_consumed_free_units + " free units consumed" );
						objUnit.parent_units.forEach
						(
							function( parent_unit )
							{
								if ( storage.assocUnstableUnits[ parent_unit ] )
								{
									storage.assocUnstableUnits[ parent_unit ].is_free = 0;
								}
							}
						)
					}
				);
			}
		
			if ( Array.isArray( objUnit.witnesses ) )
			{
				for ( let i = 0; i < objUnit.witnesses.length; i ++ )
				{
					let address	= objUnit.witnesses[ i ];
					conn.addQuery
					(
						arrQueries,
						"INSERT INTO unit_witnesses (unit, address) VALUES(?,?)",
						[
							objUnit.unit,
							address
						]
					);
				}

				conn.addQuery
				(
					arrQueries,
					"INSERT " + conn.getIgnore() + " INTO witness_list_hashes (witness_list_unit, witness_list_hash) VALUES (?,?)",
					[
						objUnit.unit,
						objectHash.getBase64Hash( objUnit.witnesses )
					]
				);
			}

			//
			//	build author addresses
			//
			let arrAuthorAddresses	= [];
			for ( let i = 0; i < objUnit.authors.length; i++ )
			{
				let author	= objUnit.authors[ i ];

				arrAuthorAddresses.push( author.address );
				let definition		= author.definition;
				let definition_chash	= null;

				if ( definition )
				{
					//	IGNORE for messages out of sequence
					definition_chash	= objectHash.getChash160( definition );

					//	...
					conn.addQuery
					(
						arrQueries,
						"INSERT " + conn.getIgnore() + " INTO definitions \n\
						( definition_chash, definition, has_references ) \n\
						VALUES (?,?,?)",
						[
							definition_chash,
							JSON.stringify( definition ),
							Definition.hasReferences( definition ) ? 1 : 0
						]
					);

					//
					//	actually inserts only when the address is first used.
					//	if we change keys and later send a unit signed by new keys, the address is not inserted.
					//	Its definition_chash was updated before when we posted change-definition message.
					//
					if ( definition_chash === author.address )
					{
						conn.addQuery
						(
							arrQueries,
							"INSERT " + conn.getIgnore() + " INTO addresses (address) VALUES(?)",
							[
								author.address
							]
						);
					}
				}
				else if ( objUnit.content_hash )
				{
					conn.addQuery
					(
						arrQueries,
						"INSERT " + conn.getIgnore() + " INTO addresses (address) VALUES(?)",
						[
							author.address
						]
					);
				}

				conn.addQuery
				(
					arrQueries,
					"INSERT INTO unit_authors ( unit, address, definition_chash ) VALUES( ?,?,? )",
					[
						objUnit.unit,
						author.address,
						definition_chash
					]
				);
				if ( bGenesis )
				{
					conn.addQuery
					(
						arrQueries,
						"UPDATE unit_authors SET _mci=0 WHERE unit=?",
						[
							objUnit.unit
						]
					);
				}

				if ( ! objUnit.content_hash )
				{
					for ( let path in author.authentifiers )
					{
						conn.addQuery
						(
							arrQueries,
							"INSERT INTO authentifiers (unit, address, path, authentifier) VALUES(?,?,?,?)",
							[
								objUnit.unit,
								author.address,
								path,
								author.authentifiers[ path ]
							]
						);
					}
				}
			}

			if ( ! objUnit.content_hash )
			{
				for ( let i = 0; i < objUnit.messages.length; i ++ )
				{
					let message		= objUnit.messages[ i ];
					let text_payload	= null;

					if ( message.app === "text" )
					{
						text_payload	= message.payload;
					}
					else if ( message.app === "data" ||
						message.app === "profile" ||
						message.app === "attestation" ||
						message.app === "definition_template" )
					{
						text_payload	= JSON.stringify( message.payload );
					}

					//
					//	...
					//
					conn.addQuery
					(
						arrQueries,
						"INSERT INTO messages \n\
						( unit, message_index, app, payload_hash, payload_location, payload, payload_uri, payload_uri_hash ) \n\
						VALUES(?,?,?,?,?,?,?,?)",
						[
							objUnit.unit,
							i,
							message.app,
							message.payload_hash,
							message.payload_location,
							text_payload,
							message.payload_uri,
							message.payload_uri_hash
						]
					);
					if ( message.payload_location === "inline" )
					{
						switch ( message.app )
						{
						case "address_definition_change":
							let definition_chash	= message.payload.definition_chash;
							let address		= message.payload.address || objUnit.authors[ 0 ].address;

							//	...
							conn.addQuery
							(
								arrQueries,
								"INSERT INTO address_definition_changes (unit, message_index, address, definition_chash) VALUES(?,?,?,?)", 
								[
									objUnit.unit,
									i,
									address,
									definition_chash
								]
							);
							break;

						case "poll":
							let poll	= message.payload;

							//	...
							conn.addQuery
							(
								arrQueries,
								"INSERT INTO polls (unit, message_index, question) VALUES(?,?,?)",
								[
									objUnit.unit,
									i,
									poll.question
								]
							);
							for ( let j = 0; j < poll.choices.length; j ++ )
							{
								conn.addQuery
								(
									arrQueries,
									"INSERT INTO poll_choices (unit, choice_index, choice) VALUES(?,?,?)",
									[
										objUnit.unit,
										j,
										poll.choices[ j ]
									]
								);
							}
							break;

						case "vote":
							let vote = message.payload;

							//	...
							conn.addQuery
							(
								arrQueries,
								"INSERT INTO votes (unit, message_index, poll_unit, choice) VALUES (?,?,?,?)",
								[
									objUnit.unit,
									i,
									vote.unit,
									vote.choice
								]
							);
							break;

						case "attestation":
							let attestation	= message.payload;

							//	...
							conn.addQuery
							(
								arrQueries,
								"INSERT INTO attestations (unit, message_index, attestor_address, address) VALUES(?,?,?,?)",
								[
									objUnit.unit,
									i,
									objUnit.authors[ 0 ].address,
									attestation.address
								]
							);

							for ( let field in attestation.profile )
							{
								let value	= attestation.profile[ field ];
								if ( field.length <= constants.MAX_PROFILE_FIELD_LENGTH &&
									typeof value === 'string' &&
									value.length <= constants.MAX_PROFILE_VALUE_LENGTH )
								{
									//	...
									conn.addQuery
									(
										arrQueries,
										"INSERT INTO attested_fields (unit, message_index, attestor_address, address, field, value) VALUES(?,?, ?,?, ?,?)",
										[
											objUnit.unit,
											i,
											objUnit.authors[ 0 ].address,
											attestation.address,
											field,
											value
										]
									);
								}
							}
							break;

						case "asset":
							let asset	= message.payload;

							//	...
							conn.addQuery
							(
								arrQueries,
								"INSERT INTO assets (unit, message_index, \n\
								cap, is_private, is_transferrable, auto_destroy, fixed_denominations, \n\
								issued_by_definer_only, cosigned_by_definer, spender_attested, \n\
								issue_condition, transfer_condition) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", 
								[
									objUnit.unit,
									i,
									asset.cap,
									asset.is_private ? 1 : 0,
									asset.is_transferrable ? 1 : 0,
									asset.auto_destroy ? 1 : 0,
									asset.fixed_denominations ? 1 : 0,
									asset.issued_by_definer_only ? 1 : 0,
									asset.cosigned_by_definer ? 1 : 0,
									asset.spender_attested ? 1 : 0,
									asset.issue_condition ? JSON.stringify( asset.issue_condition ) : null,
									asset.transfer_condition ? JSON.stringify( asset.transfer_condition ) : null
								]
							);

							if ( asset.attestors )
							{
								for ( let j = 0; j < asset.attestors.length; j ++ )
								{
									conn.addQuery
									(
										arrQueries,
										"INSERT INTO asset_attestors (unit, message_index, asset, attestor_address) VALUES(?,?,?,?)",
										[
											objUnit.unit,
											i,
											objUnit.unit,
											asset.attestors[ j ]
										]
									);
								}
							}
							if ( asset.denominations )
							{
								for ( let j = 0; j < asset.denominations.length; j ++ )
								{
									conn.addQuery
									(
										arrQueries,
										"INSERT INTO asset_denominations (asset, denomination, count_coins) VALUES(?,?,?)",
										[
											objUnit.unit,
											asset.denominations[ j ].denomination,
											asset.denominations[ j ].count_coins
										]
									);
								}
							}
							break;

						case "asset_attestors":
							let asset_attestors = message.payload;
							for ( let j = 0; j < asset_attestors.attestors.length; j ++ )
							{
								conn.addQuery
								(
									arrQueries,
									"INSERT INTO asset_attestors (unit, message_index, asset, attestor_address) VALUES(?,?,?,?)",
									[
										objUnit.unit,
										i,
										asset_attestors.asset,
										asset_attestors.attestors[ j ]
									]
								);
							}
							break;

						case "data_feed":
							let data = message.payload;
							for ( let feed_name in data )
							{
								let value	= data[ feed_name ];
								let field_name	= ( typeof value === 'string' ) ? "`value`" : "int_value";

								//	...
								conn.addQuery
								(
									arrQueries,
									"INSERT INTO data_feeds \n\
									(unit, message_index, feed_name, " + field_name + ") \n\
									VALUES(?,?,?,?)",
									[
										objUnit.unit,
										i,
										feed_name,
										value
									]
								);
							}
							break;

						case "payment":
							//	we'll add inputs/outputs later because we need to read the payer address
							//	from src outputs, and it's inconvenient to read it synchronously
							break;
						}	// switch message.app
					}	// inline

					if ( "spend_proofs" in message )
					{
						for ( let j = 0; j < message.spend_proofs.length; j ++ )
						{
							let objSpendProof	= message.spend_proofs[ j ];

							//	...
							conn.addQuery
							(
								arrQueries,
								"INSERT INTO spend_proofs \n\
								( unit, message_index, spend_proof_index, spend_proof, address ) \n\
								VALUES( ?,?,?,?,? )",
								[
									objUnit.unit,
									i,
									j,
									objSpendProof.spend_proof,
									objSpendProof.address || arrAuthorAddresses[ 0 ]
								]
							);
						}
					}
				}
			}

			if ( "earned_headers_commission_recipients" in objUnit )
			{
				for ( let i = 0; i < objUnit.earned_headers_commission_recipients.length; i++ )
				{
					let recipient	= objUnit.earned_headers_commission_recipients[ i ];

					//	...
					conn.addQuery
					(
						arrQueries,
						"INSERT INTO earned_headers_commission_recipients \n\
						( unit, address, earned_headers_commission_share ) \n\
						VALUES( ?, ?, ? )",
						[
							objUnit.unit,
							recipient.address,
							recipient.earned_headers_commission_share
						]
					);
				}
			}

			//
			//	...
			//
			let my_best_parent_unit;

			function determineInputAddressFromSrcOutput( asset, denomination, input, handleAddress )
			{
				conn.query
				(
					"SELECT address, denomination, asset FROM outputs WHERE unit=? AND message_index=? AND output_index=?",
					[
						input.unit,
						input.message_index,
						input.output_index
					],
					function( rows )
					{
						if ( rows.length > 1 )
						{
							throw Error( "multiple src outputs found" );
						}
						if ( rows.length === 0 )
						{
							if ( conf.bLight )
							{
								//	it's normal that a light client doesn't store the previous output
								return handleAddress( null );
							}
							else
							{
								throw Error( "src output not found" );
							}
						}

						let row	= rows[ 0 ];
						if ( ! ( ! asset && ! row.asset || asset === row.asset ) )
						{
							throw Error( "asset doesn't match" );
						}
						if ( denomination !== row.denomination )
						{
							throw Error( "denomination doesn't match" );
						}

						let address = row.address;
						if ( arrAuthorAddresses.indexOf( address ) === -1 )
						{
							throw Error( "src output address not among authors" );
						}

						//	...
						handleAddress( address );
					}
				);
			}

			function addInlinePaymentQueries( cb )
			{
				//
				//	async.forEachOfSeries is an alias of async.eachOfSeries
				//
				//	eachOfSeries( coll, iteratee, [opt]callback )
				//	- runs only a single async operation at a time.
				//
				//	see details in https://caolan.github.io/async/docs.html#eachOfSeries
				//
				async.forEachOfSeries
				(
					objUnit.messages,
					function( message, i, cb2 )
					{
						if ( message.payload_location !== 'inline' )
						{
							return cb2();
						}

						//	...
						let payload = message.payload;
						if ( message.app !== 'payment' )
						{
							return cb2();
						}

						//	...
						let denomination = payload.denomination || 1;

						//	...
						async.forEachOfSeries
						(
							payload.inputs,
							function( input, j, cb3 )
							{
								let type			= input.type || "transfer";
								let src_unit			= ( type === "transfer" ) ? input.unit : null;
								let src_message_index		= ( type === "transfer" ) ? input.message_index : null;
								let src_output_index		= ( type === "transfer" ) ? input.output_index : null;
								let from_main_chain_index	= ( type === "witnessing" || type === "headers_commission" ) ? input.from_main_chain_index : null;
								let to_main_chain_index		= ( type === "witnessing" || type === "headers_commission" ) ? input.to_main_chain_index : null;

								//	...
								function determineInputAddress( handleAddress )
								{
									if ( type === "headers_commission" || type === "witnessing" || type === "issue" )
									{
										return handleAddress
										(
											( arrAuthorAddresses.length === 1 ) ? arrAuthorAddresses[ 0 ] : input.address
										);
									}

									//	hereafter, transfer
									if ( arrAuthorAddresses.length === 1 )
									{
										return handleAddress( arrAuthorAddresses[ 0 ] );
									}

									//	...
									determineInputAddressFromSrcOutput
									(
										payload.asset,
										denomination,
										input,
										handleAddress
									);
								}

								//	...
								determineInputAddress
								(
									function( address )
									{
										let is_unique = objValidationState.arrDoubleSpendInputs.some
										(
											function( ds )
											{
												return ( ds.message_index === i && ds.input_index === j );
											}
										)
										? null : 1;

										conn.addQuery
										(
											arrQueries,
											"INSERT INTO inputs \n\
											(unit, message_index, input_index, type, \n\
											src_unit, src_message_index, src_output_index, \
											from_main_chain_index, to_main_chain_index, \n\
											denomination, amount, serial_number, \n\
											asset, is_unique, address) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
											[
												objUnit.unit,
												i,
												j,
												type,
									 			src_unit,
												src_message_index,
												src_output_index,
									 			from_main_chain_index,
												to_main_chain_index,
									 			denomination,
												input.amount,
												input.serial_number,
									 			payload.asset,
												is_unique,
												address
											]
										);
										switch ( type )
										{
										case "transfer":
											conn.addQuery
											(
												arrQueries,
												"UPDATE outputs SET is_spent=1 WHERE unit=? AND message_index=? AND output_index=?",
												[
													src_unit,
													src_message_index,
													src_output_index
												]
											);
											break;
										case "headers_commission":
										case "witnessing":
											let table	= type + "_outputs";
											conn.addQuery
											(
												arrQueries,
												"UPDATE " + table + " SET is_spent=1 \n\
												WHERE main_chain_index>=? AND main_chain_index<=? AND address=?",
												[
													from_main_chain_index,
													to_main_chain_index,
													address
												]
											);
											break;
										}

										//	...
										cb3();
									}
								);
							},
							function()
							{
								for ( let j = 0; j < payload.outputs.length; j++ )
								{
									let output	= payload.outputs[ j ];
									//
									//	we set is_serial=1 for public payments as we check that their inputs are stable and serial before spending,
									//	therefore it is impossible to have a nonserial in the middle of the chain (but possible for private payments)
									//
									conn.addQuery
									(
										arrQueries,
										"INSERT INTO outputs \n\
										(unit, message_index, output_index, address, amount, asset, denomination, is_serial) VALUES(?,?,?,?,?,?,?,1)",
										[
											objUnit.unit,
											i,
											j,
											output.address,
											parseInt( output.amount ),
											payload.asset,
											denomination
										]
									);
								}

								//	...
								cb2();
							}
						);
					},
					cb
				);
			}

			function updateBestParent( cb )
			{
				//	choose best parent among compatible parents only
				conn.query
				(
					"SELECT unit \n\
					FROM units AS parent_units \n\
					WHERE unit IN(?) \n\
						AND (witness_list_unit=? OR ( \n\
							SELECT COUNT(*) \n\
							FROM unit_witnesses \n\
							JOIN unit_witnesses AS parent_witnesses USING(address) \n\
							WHERE parent_witnesses.unit IN(parent_units.unit, parent_units.witness_list_unit) \n\
								AND unit_witnesses.unit IN(?, ?) \n\
						)>=?) \n\
					ORDER BY witnessed_level DESC, \n\
						level-witnessed_level ASC, \n\
						unit ASC \n\
					LIMIT 1",
					[
						objUnit.parent_units,
						objUnit.witness_list_unit,
						objUnit.unit,
						objUnit.witness_list_unit,
						constants.COUNT_WITNESSES - constants.MAX_WITNESS_LIST_MUTATIONS
					],
					function( rows )
					{
						if ( rows.length !== 1 )
						{
							throw Error( "zero or more than one best parent unit?" );
						}

						//	...
						my_best_parent_unit	= rows[ 0 ].unit;
						if ( my_best_parent_unit !== objValidationState.best_parent_unit )
							_throwError
							(
								"different best parents, validation: "
								+ objValidationState.best_parent_unit
								+ ", writer: " + my_best_parent_unit
							);

						conn.query
						(
							"UPDATE units SET best_parent_unit=? WHERE unit=?",
							[
								my_best_parent_unit,
								objUnit.unit
							],
							function()
							{
								cb();
							}
						);
					}
				);
			}

			function determineMaxLevel( handleMaxLevel )
			{
				let max_level = 0;

				//	...
				async.each
				(
					objUnit.parent_units,
					function( parent_unit, cb )
					{
						storage.readStaticUnitProps
						(
							conn,
							parent_unit,
							function( props )
							{
								if ( props.level > max_level )
								{
									max_level = props.level;
								}

								//	...
								cb();
							}
						);
					},
					function()
					{
						handleMaxLevel( max_level );
					}
				);
			}

			function updateLevel( cb )
			{
				conn.query
				(
					"SELECT MAX(level) AS max_level FROM units WHERE unit IN(?)",
					[
						objUnit.parent_units
					],
					function( rows )
					{
						if ( rows.length !== 1 )
						{
							throw Error( "not a single max level?" );
						}

						//	...
						determineMaxLevel
						(
							function( max_level )
							{
								if ( max_level !== rows[ 0 ].max_level )
									_throwError
									(
										"different max level, sql: "
										+ rows[ 0 ].max_level + ", props: " + max_level
									);

								//	...
								objNewUnitProps.level	= max_level + 1;
								conn.query
								(
									"UPDATE units SET level=? WHERE unit=?",
									[
										rows[ 0 ].max_level + 1,
										objUnit.unit
									],
									function()
									{
										cb();
									}
								);
							}
						);
					}
				);
			}

			function updateWitnessedLevel( cb )
			{
				if ( objUnit.witnesses )
				{
					updateWitnessedLevelByWitnesslist( objUnit.witnesses, cb );
				}
				else
				{
					storage.readWitnessList
					(
						conn,
						objUnit.witness_list_unit,
						function( arrWitnesses )
						{
							updateWitnessedLevelByWitnesslist( arrWitnesses, cb );
						}
					);
				}
			}

			//
			//	The level at which we collect at least 7 distinct witnesses while walking up the main chain from our unit.
			//	The unit itself is not counted even if it is authored by a witness
			//
			function updateWitnessedLevelByWitnesslist( arrWitnesses, cb )
			{
				let arrCollectedWitnesses = [];

				function setWitnessedLevel( witnessed_level )
				{
					//#profiler.start();

					if ( witnessed_level !== objValidationState.witnessed_level )
					{
						_throwError
						(
							"different witnessed levels, validation: "
							+ objValidationState.witnessed_level
							+ ", writer: " + witnessed_level
						);
					}

					//	...
					objNewUnitProps.witnessed_level	= witnessed_level;
					conn.query
					(
						"UPDATE units SET witnessed_level=? WHERE unit=?",
						[
							witnessed_level,
							objUnit.unit
						],
						function()
						{
							//#profiler.stop( 'write-wl-update' );
							cb();
						}
					);
				}

				function addWitnessesAndGoUp( start_unit )
				{
					//#profiler.start();
					storage.readStaticUnitProps
					(
						conn,
						start_unit,
						function( props )
						{
							//#profiler.stop( 'write-wl-select-bp' );

							let best_parent_unit	= props.best_parent_unit;
							let level		= props.level;

							if ( level === null )
							{
								throw Error( "null level in updateWitnessedLevel" );
							}
							if ( level === 0 )
							{
								//	genesis
								return setWitnessedLevel(0);
							}

							//#profiler.start();
							storage.readUnitAuthors
							(
								conn,
								start_unit,
								function( arrAuthors )
								{
									//#profiler.stop( 'write-wl-select-authors' );
									//#profiler.start();

									for ( let i = 0; i < arrAuthors.length; i ++ )
									{
										let address	= arrAuthors[ i ];

										if ( arrWitnesses.indexOf( address ) !== -1 &&
											arrCollectedWitnesses.indexOf( address ) === -1 )
										{
											arrCollectedWitnesses.push( address );
										}
									}

									//#profiler.stop( 'write-wl-search' );
									( arrCollectedWitnesses.length < constants.MAJORITY_OF_WITNESSES )
									?
										addWitnessesAndGoUp( best_parent_unit )
										:
										setWitnessedLevel( level );
								}
							);
						}
					);
				}

				//#profiler.stop( 'write-update' );
				addWitnessesAndGoUp( my_best_parent_unit );
			}

			//	...
			let objNewUnitProps =
				{
					unit				: objUnit.unit,
					level				: bGenesis ? 0 : null,
					latest_included_mc_index	: null,
					main_chain_index		: bGenesis ? 0 : null,
					is_on_main_chain		: bGenesis ? 1 : 0,
					is_free				: 1,
					is_stable			: bGenesis ? 1 : 0,
					witnessed_level			: bGenesis ? 0 : null,
					parent_units			: objUnit.parent_units
				};


			////////////////////////////////////////////////////////////////////////////////
			//
			//	EXECUTE
			//
			////////////////////////////////////////////////////////////////////////////////

			//
			//	without this locking, we get frequent deadlocks from mysql
			//
			mutex.lock
			(
				[ "write" ],
				function( unlock )
				{
					log.consoleLog( "got lock to write " + objUnit.unit );

					//
					//	save the unit
					//
					storage.assocUnstableUnits[ objUnit.unit ] = objNewUnitProps;

					//
					//	...
					//
					addInlinePaymentQueries
					(
						function()
						{
							async.series
							(
								arrQueries,
								function()
								{
									//#profiler.stop( 'write-raw' );

									//#profiler.start();
									let arrOps = [];

									if ( objUnit.parent_units )
									{
										if ( ! conf.bLight )
										{
											arrOps.push( updateBestParent );
											arrOps.push( updateLevel );
											arrOps.push( updateWitnessedLevel );
											arrOps.push
											(
												function( cb )
												{
													log.consoleLog( "updating MC after adding " + objUnit.unit );
													main_chain.updateMainChain( conn, null, objUnit.unit, cb );
												}
											);
										}

										if ( preCommitCallback )
										{
											arrOps.push
											(
												function( cb )
												{
													log.consoleLog( "executing pre-commit callback" );
													preCommitCallback( conn, cb );
												}
											);
										}
									}

									async.series
									(
										arrOps,
										function( err )
										{
											//#profiler.start();
											conn.query
											(
												err ? "ROLLBACK" : "COMMIT",
												function()
												{
													conn.release();
													log.consoleLog
													(
														( err ? ( err + ", therefore rolled back unit " ) : "committed unit " ) + objUnit.unit
													);

													//#profiler.stop( 'write-commit' );
													//#profiler.increment();

													if ( err )
													{
														storage.resetUnstableUnits( unlock );
													}
													else
													{
														unlock();
													}

													if ( ! err )
													{
														eventBus.emit( 'saved_unit-' + objUnit.unit, objJoint );
													}
													if ( onDone )
													{
														onDone( err );
													}

													//	...
													count_writes ++;
													if ( conf.storage === 'sqlite' )
													{
														_updateSQLiteStats();
													}
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





function _readCountOfAnalyzedUnits( handleCount )
{
	if ( count_units_in_prev_analyze )
	{
		return handleCount( count_units_in_prev_analyze );
	}

	//	...
	db.query
	(
		"SELECT * FROM sqlite_master WHERE type='table' AND name='sqlite_stat1'",
		function( rows )
		{
			if ( rows.length === 0 )
			{
				return handleCount( 0 );
			}

			db.query
			(
				"SELECT stat FROM sqlite_stat1 WHERE tbl='units' AND idx='sqlite_autoindex_units_1'",
				function( rows )
				{
					if ( rows.length !== 1 )
					{
						log.consoleLog( 'no stat for sqlite_autoindex_units_1' );
						return handleCount( 0 );
					}

					//	...
					handleCount( parseInt( rows[ 0 ].stat.split( ' ' )[ 0 ] ) );
				}
			);
		}
	);
}


/**
 *	update stats for query planner
 */
function _updateSQLiteStats()
{
	if ( count_writes === 1 )
	{
		start_time = Date.now();
		prev_time = Date.now();
	}
	if ( count_writes % 100 !== 0 )
	{
		return;
	}

	if ( count_writes % 1000 === 0 )
	{
		let total_time		= ( Date.now() - start_time ) / 1000;
		let recent_time		= ( Date.now() - prev_time ) / 1000;
		let recent_tps		= 1000 / recent_time;
		let avg_tps		= count_writes / total_time;
		prev_time		= Date.now();
		//	console.error(count_writes+" units done in "+total_time+" s, recent "+recent_tps+" tps, avg "+avg_tps+" tps");
	}

	db.query
	(
		"SELECT MAX(rowid) AS count_units FROM units",
		function( rows )
		{
			let count_units	= rows[ 0 ].count_units;
			if ( count_units > 500000 )
			{
				//	the db is too big
				return;
			}

			_readCountOfAnalyzedUnits
			(
				function( count_analyzed_units )
				{
					log.consoleLog( 'count analyzed units: ' + count_analyzed_units );
					if ( count_units < 2 * count_analyzed_units )
						return;

					count_units_in_prev_analyze	= count_units;
					log.consoleLog( "will update sqlite stats" );

					//
					//	TODO
					//	ANALYZE ? what ?
					//
					db.query
					(
						"ANALYZE",
						function()
						{
							//
							//	TODO
							//	ANALYZE sqlite_master ? what ?
							//
							db.query
							(
								"ANALYZE sqlite_master",
								function()
								{
									log.consoleLog( "sqlite stats updated" );
								}
							);
						}
					);
				}
			);
	});
}



function _throwError( msg )
{
	if ( typeof window === 'undefined' )
	{
		throw Error(msg);
	}
	else
	{
		eventBus.emit( 'nonfatal_error', msg, new Error() );
	}
}






/**
 *	exports
 */
exports.saveJoint	= saveJoint;
