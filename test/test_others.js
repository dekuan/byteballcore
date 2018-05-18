'use strict';


function test_js_map()
{
	var rows		=
	[
		{ address : 1 },
		{ address : 2 },
		{ address : 3 },
		{ address : 4 },
		{ address : 5 },
		{ address : 6 }
	];
	var arrWitnesses	= rows.map
	(
		function( row )
		{
			return row.address;
		}
	);

}



test_js_map();