/**
 *	callee library
 *	@returns	{null}
 *	@constructor
 */
function callee()
{
	var vReturnValue;
	var pfnCallback;
	var sMethod;
	var arrMethodArgs;

	if ( ! arguments || arguments.length < 2 )
	{
		throw Error( "CCallbackObject, invalid arguments" );
	}

	//	...
	vReturnValue	= null;
	pfnCallback	= arguments[ 0 ];
	sMethod		= arguments[ 1 ];
	arrMethodArgs	= Array.prototype.slice.call( arguments, 2 );

	if ( 'object' === typeof pfnCallback &&
		( 'string' === typeof sMethod && sMethod.length > 0 ) )
	{
		//
		//	call user specified method
		//
		if ( pfnCallback.hasOwnProperty( sMethod ) &&
			'function' === typeof pfnCallback[ sMethod ] )
		{
			vReturnValue	= pfnCallback[ sMethod ].apply( this, arrMethodArgs );
		}

		//
		//	We'll always be happy to call the method .finally if its existed
		//
		if ( pfnCallback.hasOwnProperty( 'finally' ) &&
			'function' === typeof pfnCallback[ 'finally' ] )
		{
			pfnCallback[ 'finally' ].apply( this, arrMethodArgs );
		}
	}

	//	...
	return vReturnValue;
}




/**
 *	exports
 */
exports.callee		= callee;

