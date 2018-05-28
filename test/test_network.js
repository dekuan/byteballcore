/**
 *	test network.js
 */
let assert      = require( 'assert' );
let crypto	= require( 'crypto' );


/**
 *      test
 */
describe( 'network.js', function()
{
	describe( 'startAcceptingConnections', function()
	{
		describe( 'crypto.randomBytes( 30 ).toString( "base64" )', function()
		{
			let sChallenge	= crypto.randomBytes( 30 ).toString( "base64" );
			it ( 'it should be a random string with length 40: ' + sChallenge, function()
			{
				assert.equal( typeof sChallenge === 'string' && 40 === sChallenge.length, true );
			});
		});
	});

});



