var _object_hash		= require( '../object_hash.js' );
var _				= require( 'lodash' );

var oObj	= {
	unit	: {
		key1	: 1,
		key2	: 1,
		key3	: 1,
		key4	: 1,
		key5	: 1,
		key6	: 1,
		key7	: 1,
		key8	: 1,
		key9	: 1
	},
	arr	: [
		1, 2, 3, 4, 5, 6, 7, 8, 9, 0
	],
	xing	: 'xing',
	'ttt'	: 1234567890
};

var oClone	= _.cloneDeep( oObj );
oClone.arr[ 0 ]	= 100;


//	...
delete oObj[ 'xing' ];
delete oObj[ 'arr' ];
delete oObj[ 'ttt' ];

console.log( oObj );


var tag	= _object_hash.getBase64Hash( oObj );

console.log( tag );


