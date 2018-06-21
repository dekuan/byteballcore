function fun1( a, b, c )
{
	console.log( this, a, b, c );
}

function fun2()
{
	fun1.apply( new Date(), arguments );
	//fun1.call( new Date(), 7, 8, 9 );
	//fun1( arguments );
}


//fun1( 1, 2, 3 );
fun2( 4, 5, 6 );

