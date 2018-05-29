/*jslint node: true */
"use strict";

var ecdsa	= require( 'secp256k1' );
var log		= require( './log.js' );



function sign( hash, priv_key )
{
	let res = ecdsa.sign( hash, priv_key );
	return res.signature.toString( "base64" );
}

function verify( hash, b64_sig, b64_pub_key )
{
	try
	{
		//	64 bytes (32+32)
		let signature = new Buffer( b64_sig, "base64" );
		return ecdsa.verify( hash, signature, new Buffer( b64_pub_key, "base64" ) );
	}
	catch( e )
	{
		log.consoleLog( 'signature verification exception: ' + e.toString() );
		return false;
	}
}





/**
 *	exports
 */
exports.sign		= sign;
exports.verify		= verify;

