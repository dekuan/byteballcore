/*jslint node: true */
"use strict";

var _				= require( 'lodash' );
var async			= require( 'async' );
var log				= require( './log.js' );
var constants			= require( './constants.js' );
var objectHash			= require( './object_hash.js' );
var paid_witnessing		= require( './paid_witnessing.js' );
var headers_commission		= require( './headers_commission.js' );
var mc_outputs			= require( './mc_outputs.js' );
var composer_const		= require( './composer_const.js' );
var conf			= require( './conf.js' );


/**
 *	member variables
 */
var m_oInstance			= null;




/**
 *	bMultiAuthored includes all addresses, not just those that pay
 *	arrAddresses is paying addresses
 */
function CPickDivisibleCoinsForAmount( oConn_, objAsset_, arrAddresses_, nLastBallMci_, nAmount_, bMultiAuthored_, pfnOnDone_ )
{
	var m_oAsset			= objAsset_ ? objAsset_.asset : null;
	var m_bIsBase			= objAsset_ ? 0 : 1;
	var m_arrInputsWithProofs	= [];
	var m_nTotalAmount		= 0;
	var m_nRequiredAmount		= nAmount_;

	//	...
	log.consoleLog( "pick coins " + m_oAsset + " amount " + nAmount_ );


	/**
	 *	handle processor
	 *	@public
	 */
	this.handle = function()
	{
		//	cloning
		var arrSpendableAddresses	= arrAddresses_.concat();
		var i;

		if ( objAsset_ &&
			objAsset_.auto_destroy )
		{
			i = arrAddresses_.indexOf( objAsset_.definer_address );
			if ( i >= 0 )
			{
				arrSpendableAddresses.splice( i, 1 );
			}
		}
		if ( arrSpendableAddresses.length > 0 )
		{
			_pickOneCoinJustBiggerAndContinue();
		}
		else
		{
			_issueAsset();
		}

		//	...
		return true;
	};


	////////////////////////////////////////////////////////////////////////////////
	//	Private
	//

	function _constructor()
	{
	}

	/**
	 *	adds element to m_arrInputsWithProofs
	 */
	function _addInput( input )
	{
		//	...
		m_nTotalAmount	+= input.amount;

		//	..
		var objInputWithProof	= { input : input };
		if ( objAsset_ && objAsset_.is_private )
		{
			//	for type=payment only
			var spend_proof		= objectHash.getBase64Hash
			(
				{
					asset		: m_oAsset,
					amount		: input.amount,
					address		: input.address,
					unit		: input.unit,
					message_index	: input.message_index,
					output_index	: input.output_index,
					blinding	: input.blinding
				}
			);
			var objSpendProof	=
				{
					spend_proof	: spend_proof
				};
			if ( bMultiAuthored_ )
			{
				objSpendProof.address	= input.address;
			}

			objInputWithProof.spend_proof	= objSpendProof;
		}

		if ( ! bMultiAuthored_ || ! input.type )
		{
			delete input.address;
		}

		delete input.amount;
		delete input.blinding;

		//	...
		m_arrInputsWithProofs.push( objInputWithProof );
	}

	//	first, try to find a coin just bigger than the required amount
	function _pickOneCoinJustBiggerAndContinue()
	{
		if ( nAmount_ === Infinity )
		{
			return _pickMultipleCoinsAndContinue();
		}

		var more	= m_bIsBase ? '>' : '>=';

		//	...
		oConn_.query
		(
			"SELECT unit, message_index, output_index, amount, blinding, address \n\
			FROM outputs \n\
			CROSS JOIN units USING(unit) \n\
			WHERE address IN(?) AND asset" + ( m_oAsset ? "=" + oConn_.escape( m_oAsset ) : " IS NULL" ) + " AND is_spent=0 AND amount " + more + " ? \n\
				AND is_stable=1 AND sequence='good' AND main_chain_index<=?  \n\
			ORDER BY amount LIMIT 1", 
			[
				arrSpendableAddresses,
				nAmount_ + m_bIsBase * composer_const.TRANSFER_INPUT_SIZE,
				nLastBallMci_
			],
			function( rows )
			{
				if ( rows.length === 1 )
				{
					var input = rows[0];

					//	default type is "transfer"
					_addInput( input );
					pfnOnDone_( m_arrInputsWithProofs, m_nTotalAmount );
				}
				else
				{
					_pickMultipleCoinsAndContinue();
				}
			}
		);
	}

	//	then, try to add smaller coins until we accumulate the target amount
	function _pickMultipleCoinsAndContinue()
	{
		//	...
		oConn_.query
		(
			"SELECT unit, message_index, output_index, amount, address, blinding \n\
			FROM outputs \n\
			CROSS JOIN units USING(unit) \n\
			WHERE address IN(?) AND asset" + ( m_oAsset ? "=" + oConn_.escape( m_oAsset ) : " IS NULL" ) + " AND is_spent=0 \n\
				AND is_stable=1 AND sequence='good' AND main_chain_index<=?  \n\
			ORDER BY amount DESC LIMIT ?",
			[
				arrSpendableAddresses,
				nLastBallMci_,
				constants.MAX_INPUTS_PER_PAYMENT_MESSAGE - 2
			],
			function( rows )
			{
				async.eachSeries
				(
					rows,
					function( row, cb )
					{
						var input	= row;

						//	...
						objectHash.cleanNulls( input );
						m_nRequiredAmount	+= m_bIsBase * composer_const.TRANSFER_INPUT_SIZE;
						_addInput( input );

						//	if we allow equality, we might get 0 amount for change which is invalid
						var bFound = m_bIsBase
							? ( m_nTotalAmount > m_nRequiredAmount )
							: ( m_nTotalAmount >= m_nRequiredAmount );
						bFound
							? cb( 'found' )
							: cb();
					},
					function( err )
					{
						if ( err === 'found' )
						{
							pfnOnDone_( m_arrInputsWithProofs, m_nTotalAmount );
						}
						else if ( m_oAsset )
						{
							_issueAsset();
						}
						else
						{
							_addHeadersCommissionInputs();
						}
					}
				);
			}
		);
	}

	function _addHeadersCommissionInputs()
	{
		_addMcInputs
		(
			"headers_commission",
			composer_const.HEADERS_COMMISSION_INPUT_SIZE,
			headers_commission.getMaxSpendableMciForLastBallMci( nLastBallMci_ ),
			_addWitnessingInputs
		);
	}

	function _addWitnessingInputs()
	{
		_addMcInputs
		(
			"witnessing",
			composer_const.WITNESSING_INPUT_SIZE,
			paid_witnessing.getMaxSpendableMciForLastBallMci( nLastBallMci_ ),
			_issueAsset
		);
	}

	function _addMcInputs( type, input_size, max_mci, onStillNotEnough )
	{
		async.eachSeries
		(
			arrAddresses_,
			function( address, cb )
			{
				var target_amount = m_nRequiredAmount + input_size + ( bMultiAuthored_ ? composer_const.ADDRESS_SIZE : 0 ) - m_nTotalAmount;

				//	...
				mc_outputs.findMcIndexIntervalToTargetAmount
				(
					oConn_,
					type,
					address,
					max_mci,
					target_amount,
					{
						ifNothing	: cb,
						ifFound		: function( from_mc_index, to_mc_index, earnings, bSufficient )
						{
							if ( earnings === 0 )
							{
								throw Error( "earnings === 0" );
							}

							//	...
							m_nTotalAmount += earnings;
							var input =
								{
									type			: type,
									from_main_chain_index	: from_mc_index,
									to_main_chain_index	: to_mc_index
								};
							var full_input_size = input_size;
							if ( bMultiAuthored_ )
							{
								// address length
								full_input_size	+= composer_const.ADDRESS_SIZE;

								//	...
								input.address = address;
							}

							//	...
							m_nRequiredAmount += full_input_size;
							m_arrInputsWithProofs.push( { input: input } );
							( m_nTotalAmount > m_nRequiredAmount )
								? cb( "found" )	//	break eachSeries
								: cb();		//	try next address
						}
					}
				);
			},
			function( err )
			{
				if ( ! err )
				{
					log.consoleLog( arrAddresses_ + " " + type + ": got only " + m_nTotalAmount + " out of required " + m_nRequiredAmount );
				}

				//	...
				( err === "found" )
					? pfnOnDone_( m_arrInputsWithProofs, m_nTotalAmount )
					: onStillNotEnough();
			}
		);
	}

	function _issueAsset()
	{
		if ( ! m_oAsset )
		{
			return _finish();
		}
		else
		{
			if ( nAmount_ === Infinity && ! objAsset_.cap )
			{
				//	don't try to create infinite issue
				return pfnOnDone_( null );
			}
		}

		//	...
		log.consoleLog( "will try to issue asset " + m_oAsset );

		//	for issue, we use full list of addresses rather than spendable addresses
		if ( objAsset_.issued_by_definer_only &&
			arrAddresses_.indexOf( objAsset_.definer_address ) === -1 )
		{
			return _finish();
		}

		var issuer_address	= objAsset_.issued_by_definer_only ? objAsset_.definer_address : arrAddresses_[0];
		var issue_amount	= objAsset_.cap || ( m_nRequiredAmount - m_nTotalAmount ) || 1;	//	1 currency unit in case m_nRequiredAmount = m_nTotalAmount


		function addIssueInput( serial_number )
		{
			m_nTotalAmount	+= issue_amount;
			var input =
				{
					type		: "issue",
					amount		: issue_amount,
					serial_number	: serial_number
				};
			if ( bMultiAuthored_ )
			{
				input.address	= issuer_address;
			}

			var objInputWithProof =
				{
					input	: input
				};
			if ( objAsset_ && objAsset_.is_private )
			{
				var spend_proof = objectHash.getBase64Hash
				(
					{
						asset		: m_oAsset,
						amount		: issue_amount,
						denomination	: 1,
						address		: issuer_address,
						serial_number	: serial_number
					}
				);
				var objSpendProof =
					{
						spend_proof	: spend_proof
					};
				if ( bMultiAuthored_ )
				{
					objSpendProof.address = input.address;
				}

				//	...
				objInputWithProof.spend_proof = objSpendProof;
			}

			//	...
			m_arrInputsWithProofs.push( objInputWithProof );
			var bFound	= m_bIsBase ? ( m_nTotalAmount > m_nRequiredAmount ) : ( m_nTotalAmount >= m_nRequiredAmount );

			//	...
			bFound
				? pfnOnDone_( m_arrInputsWithProofs, m_nTotalAmount )
				: _finish();
		}

		if ( objAsset_.cap )
		{
			//	...
			oConn_.query
			(
				"SELECT 1 FROM inputs WHERE type='issue' AND asset=?",
				[
					m_oAsset
				],
				function( rows )
				{
					if ( rows.length > 0 )
					{
						//	already issued
						return _finish();
					}

					//
					addIssueInput( 1 );
				}
			);
		}
		else
		{
			oConn_.query
			(
				"SELECT MAX(serial_number) AS max_serial_number FROM inputs WHERE type='issue' AND asset=? AND address=?", 
				[
					m_oAsset,
					issuer_address
				],
				function( rows )
				{
					var max_serial_number	= ( rows.length === 0 ) ? 0 : rows[ 0 ].max_serial_number;

					//	...
					addIssueInput( max_serial_number + 1 );
				}
			);
		}
	}

	function _finish()
	{
		if ( nAmount_ === Infinity &&
			m_arrInputsWithProofs.length > 0 )
		{
			pfnOnDone_( m_arrInputsWithProofs, m_nTotalAmount );
		}
		else
		{
			pfnOnDone_( null );
		}
	}



	//
	//	construct
	//
	_constructor();
}


/**
 *	pickDivisibleCoinsForAmount
 */
function pickDivisibleCoinsForAmount( conn, objAsset, arrAddresses, last_ball_mci, amount, bMultiAuthored, onDone )
{
	if ( ! m_oInstance instanceof CPickDivisibleCoinsForAmount )
	{
		m_oInstance = new CPickDivisibleCoinsForAmount( conn, objAsset, arrAddresses, last_ball_mci, amount, bMultiAuthored, onDone );
	}

	//	...
	return m_oInstance.handle();
}




/**
 *	exports
 */
exports.pickDivisibleCoinsForAmount			= pickDivisibleCoinsForAmount;
