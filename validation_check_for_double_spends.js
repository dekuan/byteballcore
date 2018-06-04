/*jslint node: true */
"use strict";

var _async			= require( 'async' );
var _graph			= require( './graph.js' );
var _profiler_ex		= require( './profilerex.js' );




function checkForDoubleSpends( conn, type, sql, arrSqlArgs, objUnit, objValidationState, onAcceptedDoublespends, cb )
{
	//	PPP
	_profiler_ex.begin( "validation-validateMessages-checkForDoublespends" );

	//	...
	conn.query
	(
		sql,
		arrSqlArgs,
		function( rows )
		{
			//	PPP
			_profiler_ex.end( "validation-validateMessages-checkForDoublespends" );

			//	...
			if ( rows.length === 0 )
			{
				return cb();
			}

			var arrAuthorAddresses = objUnit.authors.map
			(
				function( author )
				{
					return author.address;
				}
			);


			//	PPP
			_profiler_ex.begin( "validation-validateMessages-checkForDoublespends-async.eachSeries" );

			//	...
			_async.eachSeries
			(
				rows,
				function( objConflictingRecord, cb2 )
				{
					if ( arrAuthorAddresses.indexOf( objConflictingRecord.address ) === -1 )
					{
						throw Error( "conflicting " + type + " spent from another address?" );
					}

					//	PPP
					_profiler_ex.begin( "validation-validateMessages-checkForDoublespends-async.eachSeries-graph.determineIfIncludedOrEqual" );

					//	...
					_graph.determineIfIncludedOrEqual
					(
						conn,
						objConflictingRecord.unit,
						objUnit.parent_units,
						function( bIncluded )
						{
							//	PPP
							_profiler_ex.end( "validation-validateMessages-checkForDoublespends-async.eachSeries-graph.determineIfIncludedOrEqual" );

							if ( bIncluded )
							{
								var error	= objUnit.unit + ": conflicting " + type + " in inner unit " + objConflictingRecord.unit;

								//	too young (serial or nonserial)
								if ( objConflictingRecord.main_chain_index > objValidationState.last_ball_mci ||
									objConflictingRecord.main_chain_index === null )
								{
									return cb2( error );
								}

								//	in good sequence (final state)
								if ( objConflictingRecord.sequence === 'good' )
								{
									return cb2( error );
								}

								//	to be voided: can reuse the output
								if ( objConflictingRecord.sequence === 'final-bad' )
								{
									return cb2();
								}

								throw Error
								(
									"unreachable code, conflicting " + type + " in unit " + objConflictingRecord.unit
								);
							}
							else
							{
								//	arrAddressesWithForkedPath is not set when validating private payments
								if ( objValidationState.arrAddressesWithForkedPath &&
									objValidationState.arrAddressesWithForkedPath.indexOf( objConflictingRecord.address ) === -1 )
								{
									throw Error( "double spending " + type +" without double spending address?" );
								}

								//	...
								cb2();
							}
						}
					);
				},
				function( err )
				{
					//	PPP
					_profiler_ex.end( "validation-validateMessages-checkForDoublespends-async.eachSeries" );

					if ( err )
					{
						return cb( err );
					}

					//
					//	TODO
					//	accept double spends?
					//
					onAcceptedDoublespends( cb );
				}
			);
		}
	);
}





/**
 *	exports
 */
exports.checkForDoubleSpends	= checkForDoubleSpends;
