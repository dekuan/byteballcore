var util	= require('util');

var args	= [
	{
		key1	: "key1",
		key2	: "key2",
		key3	: "string"
	},
	"사랑해요",
	Date.now(),
	( new Date() ).toLocaleString(),
	[
		1, 2, 3, 4, 5
	],
	null,
	new Date()
];

console.log( util.format( args ) );

