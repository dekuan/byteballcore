var _logex	= require( '../logex' );


var m_cCDACache	= new _logex.CDoubleArrayCache();
var m_arrTmp	= null;

console.log( '############################################################' );



m_arrTmp	= m_cCDACache.extract();
console.log( 'NO.0', m_arrTmp );

//
//	push logs to online piece
//
m_cCDACache.push( { tm : new Date(), args : [ 1, 2, 3 ] } );

m_arrTmp	= m_cCDACache.extract();
console.log( 'NO.1', m_arrTmp );

m_cCDACache.push( { tm : new Date(), args : [ 4, 5, 6 ] } );


m_arrTmp	= m_cCDACache.extract();
console.log( 'NO.2', m_arrTmp );

m_cCDACache.push( { tm : new Date(), args : [ 7, 7, 7 ] } );
m_cCDACache.push( { tm : new Date(), args : [ 8, 8, 8 ] } );

m_cCDACache.extract( function ( arrList )
{
	m_arrTmp	= arrList;
	console.log( 'NO.3 callback', m_arrTmp );
} );



//	...
m_arrTmp	= m_cCDACache.extract();
console.log( 'THE END 1', m_arrTmp );

m_arrTmp	= m_cCDACache.extract();
console.log( 'THE END 2', m_arrTmp );

m_arrTmp	= m_cCDACache.extract();
console.log( 'THE END 3', m_arrTmp );










