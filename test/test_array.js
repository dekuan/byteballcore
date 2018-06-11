var m_arrCache		= [];
var m_arrCacheLeft	= [];
var m_arrCacheRight	= [];


m_arrCache	= m_arrCacheLeft;
m_arrCache.push( 1 );
m_arrCache.push( 2 );
m_arrCache.push( 3 );

m_arrCache	= m_arrCacheRight;
m_arrCache.push( 4 );
m_arrCache.push( 5 );
m_arrCache.push( 6 );

m_arrCache	= null;
m_arrCacheLeft.shift();

console.log( m_arrCache, m_arrCacheLeft, m_arrCacheRight );


