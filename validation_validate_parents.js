var _				= require( 'lodash' );
var _async			= require( 'async' );
var _graph			= require( './graph.js' );
var _main_chain			= require( './main_chain.js' );
var _object_hash		= require( './object_hash.js' );
var _constants			= require( './constants.js' );
var _validation_utils		= require( './validation_utils.js' );




/**
 *	class CValidateParents
 *
 *	@param	conn_
 *	@param	objJoint_
 *	@param	objValidationState_
 *	@param	callback_
 *	@returns {*}
 *	@constructor
 */
function CValidateParents( conn_, objJoint_, objValidationState_, callback_ )
{
	var m_pfnCreateError		= null;
	var m_sLastBall			= null;
	var m_sLastBallUnit		= null;
	var m_sPrev			= null;
	var m_arrMissingParentUnits	= [];
	var m_arrPrevParentUnitProps	= [];
	var m_sJoin			= null;
	var m_sField			= null;



	/**
	 *	@public
	 */
	this.handle = function()
	{
		if ( objJoint_.unit.parent_units.length > _constants.MAX_PARENTS_PER_UNIT )
		{
			//	anti-spam
			return callback_( "too many parents: " + objJoint_.unit.parent_units.length );
		}

		//
		//	obsolete: when handling a ball, we can't trust parent list before we verify ball hash
		//	obsolete: when handling a fresh unit, we can begin trusting parent list earlier, after we verify parents_hash
		//
		m_pfnCreateError = objJoint_.ball
			? _validation_utils.createJointError
			: function( err )
			{
				return err;
			};

		//
		//	after this point, we can trust parent list as it either agrees with parents_hash or agrees with hash tree
		//	hence, there are no more joint errors, except unordered parents or skiplist units
		//
		m_sLastBall			= objJoint_.unit.last_ball;
		m_sLastBallUnit			= objJoint_.unit.last_ball_unit;
		m_sPrev				= "";
		m_arrMissingParentUnits		= [];
		m_arrPrevParentUnitProps	= [];

		//	...
		m_sJoin		= objJoint_.ball ? 'LEFT JOIN balls USING(unit) LEFT JOIN hash_tree_balls ON units.unit=hash_tree_balls.unit' : '';
		m_sField	= objJoint_.ball ? ', IFNULL(balls.ball, hash_tree_balls.ball) AS ball' : '';

		//	...
		objValidationState_.max_parent_limci	= 0;

		//
		//	...
		//
		_async.eachSeries
		(
			objJoint_.unit.parent_units,
			_parentUnitsIteratee,
			_parentUnitsCallback
		);
	};



	//	--------------------------------------------------------------------------------
	//	Private
	//	--------------------------------------------------------------------------------
	function _constructor()
	{
	}


	/**
	 *	ineratee function of objJoint_.unit.parent_units
	 *
	 *	@param parent_unit
	 *	@param cb
	 *	@returns {*}
	 *	@private
	 */
	function _parentUnitsIteratee( parent_unit, cb )
	{
		if ( parent_unit <= m_sPrev )
		{
			return cb( m_pfnCreateError( "parent units not ordered" ) );
		}

		//	...
		m_sPrev	= parent_unit;

		//	...
		conn_.query
		(
			"SELECT units.*" + m_sField + " FROM units " + m_sJoin + " WHERE units.unit=?",
			[
				parent_unit
			],
			function( rows )
			{
				if ( rows.length === 0 )
				{
					m_arrMissingParentUnits.push( parent_unit );
					return cb();
				}

				var objParentUnitProps	= rows[0];

				//	already checked in _validateHashTree that the parent ball is known, that's why we throw
				if ( objJoint_.ball && objParentUnitProps.ball === null )
				{
					throw Error( "no ball corresponding to parent unit " + parent_unit );
				}
				if ( objParentUnitProps.latest_included_mc_index > objValidationState_.max_parent_limci )
				{
					objValidationState_.max_parent_limci = objParentUnitProps.latest_included_mc_index;
				}

				//	...
				_async.eachSeries
				(
					m_arrPrevParentUnitProps,
					function( objPrevParentUnitProps, cb2 )
					{
						_graph.compareUnitsByProps
						(
							conn_,
							objPrevParentUnitProps,
							objParentUnitProps,
							function( result )
							{
								( result === null )
									?
									cb2()
									: cb2( "parent unit " + parent_unit + " is related to one of the other parent units" );
							}
						);
					},
					function( err )
					{
						if ( err )
						{
							return cb( err );
						}

						//	...
						m_arrPrevParentUnitProps.push( objParentUnitProps );
						cb();
					}
				);
			}
		);
	}

	/**
	 *	finally callback function of objJoint_.unit.parent_units
	 *	@param err
	 *	@returns {*}
	 *	@private
	 */
	function _parentUnitsCallback( err )
	{
		if ( err )
		{
			return callback_( err );
		}

		if ( m_arrMissingParentUnits.length > 0 )
		{
			conn_.query
			(
				"SELECT error FROM known_bad_joints WHERE unit IN(?)",
				[
					m_arrMissingParentUnits
				],
				function( rows )
				{
					( rows.length > 0 )
						? callback_
						(
							"some of the unit's parents are known bad: " + rows[ 0 ].error
						)
						: callback_
						(
							{
								error_code	: "unresolved_dependency",
								arrMissingUnits	: m_arrMissingParentUnits
							}
						);
				}
			);
			return;
		}

		//
		//	this is redundant check, already checked in _validateHashTree()
		//
		if ( objJoint_.ball )
		{
			var arrParentBalls = m_arrPrevParentUnitProps.map
			(
				function( objParentUnitProps )
				{
					return objParentUnitProps.ball;
				}
			).sort();

			//if (arrParentBalls.indexOf(null) === -1){
			var hash = _object_hash.getBallHash
			(
				objJoint_.unit.unit,
				arrParentBalls,
				objValidationState_.arrSkiplistBalls,
				!! objJoint_.unit.content_hash
			);
			if ( hash !== objJoint_.ball )
			{
				//	shouldn't happen, already validated in _validateHashTree()
				throw Error( "ball hash is wrong" );
			}
			//}
		}

		//	...
		conn_.query
		(
			"SELECT is_stable, is_on_main_chain, main_chain_index, ball, (SELECT MAX(main_chain_index) FROM units) AS max_known_mci \n\
			FROM units LEFT JOIN balls USING(unit) WHERE unit=?",
			[
				m_sLastBallUnit
			],
			function( rows )
			{
				if ( rows.length !== 1 )
				{
					//	at the same time, direct parents already received
					return callback_( "last ball unit " + m_sLastBallUnit + " not found" );
				}

				//	...
				var objLastBallUnitProps	= rows[ 0 ];

				//
				//	it can be unstable and have a received (not self-derived) ball
				//	if (objLastBallUnitProps.ball !== null && objLastBallUnitProps.is_stable === 0)
				//		throw "last ball "+m_sLastBall+" is unstable";
				//
				if ( objLastBallUnitProps.ball === null &&
					objLastBallUnitProps.is_stable === 1 )
				{
					throw Error( "last ball unit " + m_sLastBallUnit + " is stable but has no ball" );
				}
				if ( objLastBallUnitProps.is_on_main_chain !== 1 )
				{
					return callback_( "last ball " + m_sLastBall + " is not on MC" );
				}
				if ( objLastBallUnitProps.ball && objLastBallUnitProps.ball !== m_sLastBall )
				{
					return callback_( "last_ball " + m_sLastBall + " and last_ball_unit " + m_sLastBallUnit + " do not match" );
				}

				//	...
				objValidationState_.last_ball_mci	= objLastBallUnitProps.main_chain_index;
				objValidationState_.max_known_mci	= objLastBallUnitProps.max_known_mci;
				if ( objValidationState_.max_parent_limci < objValidationState_.last_ball_mci )
				{
					return callback_( "last ball unit " + m_sLastBallUnit + " is not included in parents, unit " + objJoint_.unit.unit );
				}
				if ( objLastBallUnitProps.is_stable === 1 )
				{
					//	if it were not stable, we wouldn't have had the ball at all
					if ( objLastBallUnitProps.ball !== m_sLastBall )
					{
						return callback_( "stable: last_ball " + m_sLastBall + " and last_ball_unit " + m_sLastBallUnit + " do not match" );
					}
					if ( objValidationState_.last_ball_mci <= 1300000 )
					{
						return _checkNoSameAddressInDifferentParents();
					}
				}

				//	Last ball is not stable yet in our view. Check if it is stable in view of the parents
				_main_chain.determineIfStableInLaterUnitsAndUpdateStableMcFlag
				(
					conn_,
					m_sLastBallUnit,
					objJoint_.unit.parent_units,
					objLastBallUnitProps.is_stable,
					function( bStable )
					{
						/*if (!bStable && objLastBallUnitProps.is_stable === 1){
							var eventBus = require('./event_bus.js');
							eventBus.emit('nonfatal_error', "last ball is stable, but not stable in parents, unit "+objJoint_.unit.unit, new Error());
							return _checkNoSameAddressInDifferentParents();
						}
						else */
						if ( ! bStable )
						{
							return callback_
							(
								objJoint_.unit.unit + ": last ball unit " + m_sLastBallUnit + " is not stable in view of your parents " + objJoint_.unit.parent_units
							);
						}

						//	...
						conn_.query
						(
							"SELECT ball FROM balls WHERE unit=?",
							[
								m_sLastBallUnit
							],
							function( ball_rows )
							{
								if ( ball_rows.length === 0 )
								{
									throw Error( "last ball unit " + m_sLastBallUnit + " just became stable but ball not found" );
								}
								if ( ball_rows[ 0 ].ball !== m_sLastBall )
								{
									return callback_
									(
										"last_ball " + m_sLastBall + " and last_ball_unit "
										+ m_sLastBallUnit + " do not match after advancing stability point"
									);
								}

								//	...
								_checkNoSameAddressInDifferentParents();
							}
						);
					}
				);
			}
		);
	}


	//
	//	avoid merging the obvious nonserials
	//
	function _checkNoSameAddressInDifferentParents()
	{
		if ( objJoint_.unit.parent_units.length === 1 )
		{
			return _checkLastBallDidNotRetreat();
		}

		//	...
		conn_.query
		(
			"SELECT address, COUNT(*) AS c FROM unit_authors WHERE unit IN(?) GROUP BY address HAVING c>1",
			[
				objJoint_.unit.parent_units
			],
			function( rows )
			{
				if ( rows.length > 0 )
				{
					return callback_( "some addresses found more than once in parents, e.g. " + rows[ 0 ].address );
				}

				//	...
				return _checkLastBallDidNotRetreat();
			}
		);
	}

	function _checkLastBallDidNotRetreat()
	{
		conn_.query
		(
			"SELECT MAX(lb_units.main_chain_index) AS max_parent_last_ball_mci \n\
			FROM units JOIN units AS lb_units ON units.last_ball_unit=lb_units.unit \n\
			WHERE units.unit IN(?)",
			[
				objJoint_.unit.parent_units
			],
			function( rows )
			{
				var max_parent_last_ball_mci	= rows[ 0 ].max_parent_last_ball_mci;

				if ( max_parent_last_ball_mci > objValidationState_.last_ball_mci )
				{
					return callback_( "last ball mci must not retreat, parents: " + objJoint_.unit.parent_units.join( ', ' ) );
				}

				//	...
				callback_();
			}
		);
	}


	//
	//	...
	//
	_constructor();
}




/**
 *	exports
 */
exports.CValidateParents	= CValidateParents;

