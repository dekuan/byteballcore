/**
 *	test for storage
 */
var assert      = require( 'assert' );
var async	= require( 'async' );


//
// var arrResultList	= [];
// async.series
// (
// 	[
// 		function( pfnCallback )
// 		{
// 			arrResultList.push( 1 );
// 			console.log( 'task[ 0 ] . ' + JSON.stringify( arrResultList ) );
//
// 			//	...
// 			return pfnCallback();
// 		},
// 		function( pfnCallback )
// 		{
// 			arrResultList.push( 2 );
// 			console.log( 'task[ 1 ] . ' + JSON.stringify( arrResultList ) );
//
// 			//	...
// 			return pfnCallback();
// 		},
// 		function( pfnCallback )
// 		{
// 			arrResultList.push( 3 );
// 			console.log( 'task[ 2 ] . ' + JSON.stringify( arrResultList ) );
//
// 			//	...
// 			return pfnCallback();
// 		}
// 	],
// 	function( err )
// 	{
// 		arrResultList.push( 4 );
// 		console.log( err, 'callback . ' + JSON.stringify( arrResultList ) );
// 	}
// );



describe( 'storage.js', function()
{
	describe( 'any testing', function()
	{
		describe( 'async.series', function()
		{
			var arrResultList	= [];

			async.series
			(
				[
					function( pfnCallback )
					{
						arrResultList.push( 1 );
						pfnCallback();

						it ( 'task[ 0 ] . ' + JSON.stringify( arrResultList ), function( pfnDone )
						{
							pfnDone();
						});
					},
					function( pfnCallback )
					{
						arrResultList.push( 2 );
						pfnCallback();

						it ( 'task[ 1 ] . ' + JSON.stringify( arrResultList ), function( pfnDone )
						{
							pfnDone();
						});
					},
					function( pfnCallback )
					{
						arrResultList.push( 3 );
						pfnCallback();

						it ( 'task[ 2 ] . ' + JSON.stringify( arrResultList ), function( pfnDone )
						{
							pfnDone();
						});
					}
				],
				function( err )
				{
					arrResultList.push( 4 );

					it ( 'callback . ' + JSON.stringify( arrResultList ), function( pfnDone )
					{
						pfnDone();
					});
				}
			);

			it ( 'arrResultList should be a array with length 4 : ' + JSON.stringify( arrResultList ), function( pfnDone )
			{
				assert.equal( Array.isArray( arrResultList ), true );
				pfnDone();
			});
		});
	});

});

