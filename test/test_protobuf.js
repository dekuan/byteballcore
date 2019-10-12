var _protobuf = require( "protobufjs" );



_protobuf.load
(
	"test_protobuf.proto",
	function( err, root )
	{
		if ( err )
		{
			throw err;
		}

		// Obtain a message type
		var TrustNoteP2pSaying = root.lookupType( "trust_note_p2p_saying_package.TrustNoteP2pSaying" );

		//	Exemplary payload
		var oPayloadNormal	=
			{
				type	: "saying",
				subject	: "subscribe",
				body	: "hello"
			};
		var oPayloadWithOmitting	=
			{
				subject	: "subscribe",
				body	: "hello"
			};

		//	Verify the oPayload if necessary (i.e. when possibly incomplete or invalid)
		var errMsgNormal	= TrustNoteP2pSaying.verify( oPayloadNormal );
		var errMsgWithOmitting	= TrustNoteP2pSaying.verify( oPayloadWithOmitting );
		if ( errMsgNormal )
		{
			throw Error( errMsgNormal );
		}
		if ( errMsgWithOmitting )
		{
			throw Error( errMsgWithOmitting );
		}

		//
		//	Create a new message
		//	or use .fromObject if conversion is necessary
		//
		var oMessage 			= TrustNoteP2pSaying.create( oPayloadNormal );
		var oMessageWithOmitting 	= TrustNoteP2pSaying.create( oPayloadWithOmitting );

		//
		// 	Encode a message to an Uint8Array (browser) or Buffer (node)
		//
		var oEncodedBuffer		= TrustNoteP2pSaying.encode( oMessage ).finish();
		var oEncodedBufferWithOmitting	= TrustNoteP2pSaying.encode( oMessageWithOmitting ).finish();
		//	... do something with buffer

		//	Decode an Uint8Array (browser) or Buffer (node) to a message
		var oDecodeMessage		= TrustNoteP2pSaying.decode( oEncodedBuffer );
		var oDecodeMessageWithOmitting	= TrustNoteP2pSaying.decode( oEncodedBufferWithOmitting );
		//	... do something with message

		var sTypeWithOmitting		= oDecodeMessageWithOmitting.type;


		//	If the application uses length-delimited buffers, there is also encodeDelimited and decodeDelimited.

		//
		//	Convert the message back to a plain object / Javascript object
		//
		var oPlainObject = TrustNoteP2pSaying.toObject( oDecodeMessage, {
			longs: String,
			enums: String,
			bytes: String,
			// see ConversionOptions
		});
	}
);