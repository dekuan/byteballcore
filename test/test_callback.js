var g_nFlag	= 0;

0 === g_nFlag ++
	? function ()
	{
		console.log( "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" );
		console.log( "g_nFlag should be 0" );
	}()
	: function ()
	{
		console.log( "########################################" );
		console.log( "g_nFlag is not 0" );
	}();

1 === g_nFlag ++
	? function ()
	{
		console.log( "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" );
		console.log( "g_nFlag should be 1" );
	}()
	: function ()
	{
		console.log( "########################################" );
		console.log( "g_nFlag is not 1" );
	}();