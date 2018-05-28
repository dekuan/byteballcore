/**
 *	test for storage
 */
let assert      = require( 'assert' );
var async	= require('async');



/**
 *      test
 */
describe( 'storage.js', function()
{
	describe( 'global testing', function()
	{
		describe( 'async.series', function()
		{
			let sChallenge	= crypto.randomBytes( 30 ).toString( "base64" );
			it ( 'it should be a random string with length 40: ' + sChallenge, function()
			{
				assert.equal( typeof sChallenge === 'string' && 40 === sChallenge.length, true );
			});
		});
	});

});

