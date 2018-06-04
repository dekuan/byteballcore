/*jslint node: true */
"use strict";

var _storage			= require( './storage.js' );
var _chash			= require( './chash.js' );
var _constants			= require( './constants.js' );
var _conf			= require( './conf.js' );
var _profiler_ex		= require( './profilerex.js' );



/**
 *	CValidateWitnesses
 *
 *	@param conn_
 *	@param objUnit_
 *	@param objValidationState_
 *	@param callback_
 *	@returns {*}
 *	@constructor
 */
function CValidateWitnesses( conn_, objUnit_, objValidationState_, callback_ )
{
	var m_sLastBallUnit;


	/**
	 *	handle
	 *	@returns {*}
	 */
	this.handle = function()
	{
		//	...
		_profiler_ex.begin( 'validation-witnesses-read-list' );

		//	...
		m_sLastBallUnit	= objUnit_.last_ball_unit;

		//	...
		if ( "string" === typeof objUnit_.witness_list_unit )
		{
			//	...
			conn_.query
			(
				"SELECT sequence, is_stable, main_chain_index FROM units WHERE unit=?",
				[
					objUnit_.witness_list_unit
				],
				function( unit_rows )
				{
					if ( unit_rows.length === 0 )
					{
						return _callback
						(
							"witness list unit " + objUnit_.witness_list_unit + " not found"
						);
					}

					//	...
					var objWitnessListUnitProps	= unit_rows[ 0 ];

					if ( objWitnessListUnitProps.sequence !== 'good' )
					{
						return _callback( "witness list unit " + objUnit_.witness_list_unit + " is not serial" );
					}
					if ( objWitnessListUnitProps.is_stable !== 1 )
					{
						return _callback( "witness list unit " + objUnit_.witness_list_unit + " is not stable" );
					}
					if ( objWitnessListUnitProps.main_chain_index > objValidationState_.last_ball_mci )
					{
						return _callback( "witness list unit " + objUnit_.witness_list_unit + " must come before last ball" );
					}

					//	...
					_storage.readWitnessList
					(
						conn_,
						objUnit_.witness_list_unit,
						function( arrWitnesses )
						{
							if ( arrWitnesses.length === 0 )
							{
								return _callback( "referenced witness list unit " + objUnit_.witness_list_unit + " has no witnesses" );
							}

							//	...
							_profiler_ex.end( 'validation-witnesses-read-list' );
							_validateWitnessListMutations( arrWitnesses );
						},
						true
					);
				}
			);
		}
		else if ( Array.isArray( objUnit_.witnesses ) &&
			objUnit_.witnesses.length === _constants.COUNT_WITNESSES )
		{
			var prev_witness	= objUnit_.witnesses[ 0 ];

			for ( var i = 0; i < objUnit_.witnesses.length; i++ )
			{
				var curr_witness	= objUnit_.witnesses[ i ];

				if ( ! _chash.isChashValid( curr_witness ) )
				{
					return _callback( "witness address " + curr_witness + " is invalid" );
				}
				if ( i === 0 )
				{
					continue;
				}
				if ( curr_witness <= prev_witness )
				{
					return _callback( "wrong order of witnesses, or duplicates" );
				}

				//	...
				prev_witness	= curr_witness;
			}

			if ( _storage.isGenesisUnit( objUnit_.unit ) )
			{
				//	addresses might not be known yet, it's ok
				_validateWitnessListMutations( objUnit_.witnesses );
				return;
			}


			//	...
			_profiler_ex.begin( 'validation-witnesses-stable' );

			//	check that all witnesses are already known and their units are good and stable
			conn_.query
			(
				//	address=definition_chash is true in the first appearence of the address
				//	(not just in first appearence: it can return to its initial definition_chash sometime later)
				"SELECT COUNT(DISTINCT address) AS count_stable_good_witnesses FROM unit_authors JOIN units USING(unit) \n\
				WHERE address=definition_chash AND +sequence='good' AND is_stable=1 AND main_chain_index<=? AND address IN(?)",
				[
					objValidationState_.last_ball_mci,
					objUnit_.witnesses
				],
				function( rows )
				{
					if ( rows[ 0 ].count_stable_good_witnesses !== _constants.COUNT_WITNESSES )
					{
						return _callback( "some witnesses are not stable, not serial, or don't come before last ball" );
					}

					//	...
					_profiler_ex.end( 'validation-witnesses-stable' );

					//	...
					_validateWitnessListMutations( objUnit_.witnesses );
				}
			);
		}
		else
		{
			return _callback( "no witnesses or not enough witnesses" );
		}
	};




	//	--------------------------------------------------------------------------------
	//	Private
	//	--------------------------------------------------------------------------------
	function _constructor()
	{
	}


	function _validateWitnessListMutations( arrWitnesses )
	{
		if ( ! objUnit_.parent_units )
		{
			//	genesis
			return _callback();
		}

		//	...
		_storage.determineIfHasWitnessListMutationsAlongMc
		(
			conn_,
			objUnit_,
			m_sLastBallUnit,
			arrWitnesses,
			function( err )
			{
				if ( err &&
					objValidationState_.last_ball_mci >= 512000 )
				{
					//	do not enforce before the || bug was fixed
					return _callback( err );
				}

				//	...
				_checkNoReferencesInWitnessAddressDefinitions( arrWitnesses );
			}
		);
	}

	function _checkNoReferencesInWitnessAddressDefinitions( arrWitnesses )
	{
		_profiler_ex.begin( 'validation-checkNoReferencesInWitnessAddressDefinitions' );

		//	correct the query planner
		var cross	= ( _conf.storage === 'sqlite' ) ? 'CROSS' : '';

		//	...
		conn_.query
		(
			"SELECT 1 \n\
			FROM address_definition_changes \n\
			JOIN definitions USING(definition_chash) \n\
			JOIN units AS change_units USING(unit)   -- units where the change was declared \n\
			JOIN unit_authors USING(definition_chash) \n\
			JOIN units AS definition_units ON unit_authors.unit=definition_units.unit   -- units where the definition was disclosed \n\
			WHERE address_definition_changes.address IN(?) AND has_references=1 \n\
				AND change_units.is_stable=1 AND change_units.main_chain_index<=? AND +change_units.sequence='good' \n\
				AND definition_units.is_stable=1 AND definition_units.main_chain_index<=? AND +definition_units.sequence='good' \n\
			UNION \n\
			SELECT 1 \n\
			FROM definitions \n\
			" + cross + " JOIN unit_authors USING(definition_chash) \n\
			JOIN units AS definition_units ON unit_authors.unit=definition_units.unit   -- units where the definition was disclosed \n\
			WHERE definition_chash IN(?) AND has_references=1 \n\
				AND definition_units.is_stable=1 AND definition_units.main_chain_index<=? AND +definition_units.sequence='good' \n\
			LIMIT 1",
			[
				arrWitnesses,
				objValidationState_.last_ball_mci,
				objValidationState_.last_ball_mci,
				arrWitnesses,
				objValidationState_.last_ball_mci
			],
			function( rows )
			{
				//profiler.stop( 'validation-witnesses-no-refs' );
				_profiler_ex.end( 'validation-checkNoReferencesInWitnessAddressDefinitions' );

				( rows.length > 0 )
					? _callback( "some witnesses have references in their addresses" )
					: _checkWitnessedLevelDidNotRetreat( arrWitnesses );
			}
		);
	}


	function _checkWitnessedLevelDidNotRetreat( arrWitnesses )
	{
		_storage.determineWitnessedLevelAndBestParent
		(
			conn_,
			objUnit_.parent_units,
			arrWitnesses,
			function( witnessed_level, best_parent_unit )
			{
				objValidationState_.witnessed_level	= witnessed_level;
				objValidationState_.best_parent_unit	= best_parent_unit;

				if ( objValidationState_.last_ball_mci < 1400000 )
				{
					//	not enforced
					return _callback();
				}

				//	...
				_storage.readStaticUnitProps
				(
					conn_,
					best_parent_unit,
					function( props )
					{
						( witnessed_level >= props.witnessed_level )
							? _callback()
							: _callback( "witnessed level retreats from " + props.witnessed_level + " to " + witnessed_level );
					}
				);
			}
		);
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
			console.log( "CValidateWitnesses::_callback", vError );
		}
		else
		{
			console.log( "CValidateWitnesses::_callback - @successfully" );
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
exports.CValidateWitnesses	= CValidateWitnesses;
