/*jslint node: true */
"use strict";


/**
 *	exports
 */
exports.TRANSFER_INPUT_SIZE = 0			//	type: "transfer" omitted
	+ 44					//	unit
	+ 8					//	message_index
	+ 8;					//	output_index

exports.HEADERS_COMMISSION_INPUT_SIZE	= 18	//	type: "headers_commission"
	+ 8					//	from_main_chain_index
	+ 8;					//	to_main_chain_index

exports.WITNESSING_INPUT_SIZE = 10		//	type: "witnessing"
	+ 8					//	from_main_chain_index
	+ 8;					//	to_main_chain_index



exports.ADDRESS_SIZE		= 32;

exports.TYPICAL_FEE		= 1000;
exports.MAX_FEE			= 20000;

exports.HASH_PLACEHOLDER	= "--------------------------------------------";	//	256 bits (32 bytes) base64: 44 bytes
exports.SIG_PLACEHOLDER		= "----------------------------------------------------------------------------------------";	//	88 bytes
