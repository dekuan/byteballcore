var _crypto	= require( 'crypto' );


var btRandom	= _crypto.randomBytes( 30 );
var sChallenge	= btRandom.toString( "base64" );

console.log( btRandom, sChallenge );

