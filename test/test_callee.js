var callee	= require( '../callee' );






function main_procedure( callback )
{
	var vReturnValue;

	//	...
	vReturnValue	= callee.callee( callback, 'ifUnitError', 'error description and id' );

	//	...
	vReturnValue	= callee.callee( callback, 'ifOk', { 'objValidationState' : 1 }, function()
	{
	});

	console.log( 'vReturnValue = ' + vReturnValue );
}


main_procedure
(
	{
		ifUnitError : function( error )
		{
			console.log( '.ifUnitError was called', arguments );
			return 1;
		},
		ifJointError : function( error )
		{
			console.log( '.ifJointError was called', arguments );
			return 2;
		},
		ifOk : function( objValidationState, validation_unlock )
		{
			console.log( '.ifOk was called', arguments );
			return 3;
		},
		finally : function()
		{
			console.log( '.finally was called', arguments );
			return 100;
		}
	}
);

