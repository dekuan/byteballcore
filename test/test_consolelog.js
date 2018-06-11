
function test_console_log0()
{
}

function test_console_log1()
{
	console.log( "1" );
}

function test_console_log2()
{
	console.log
	(
		"111111",
		Date.now(),
		( new Date() ).toLocaleDateString(),
		( new Date() ).toLocaleDateString(),
		( new Date() ).toLocaleDateString(),
		( new Date() ).toLocaleString(),
		( new Date() ).toLocaleTimeString(),
		( new Date() ).toLocaleDateString(),
		( new Date() ).toLocaleDateString(),
		( new Date() ).toLocaleDateString()
	);
}



var nStart, nUsed0, nUsed1, nUsed2;
var i;


nStart	= Date.now();
for ( i = 0; i < 10000; i ++ )
{
	test_console_log0();
}
nUsed0	= Date.now() - nStart;


//	...
nStart	= Date.now();
for ( i = 0; i < 10000; i ++ )
{
	test_console_log1();
}
nUsed1	= Date.now() - nStart;


//	...
nStart	= Date.now();
for ( i = 0; i < 10000; i ++ )
{
	test_console_log2();
}
nUsed2	= Date.now() - nStart;




console.log( "nUsed0=", nUsed0, "nUsed1=", nUsed1, "nUsed2=", nUsed2 );




