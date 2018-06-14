var _fs		= require( 'fs' );
var _desktop	= require( '../desktop_app.js' );


//	...
var m_sAppDataDir	= _desktop.getAppDataDir() + '/aaa';
var m_bAppDataDirExists	= _fs.existsSync( m_sAppDataDir );

console.log( 'm_sAppDataDir : ', m_sAppDataDir );
console.log( 'm_bAppDataDirExists : ', m_bAppDataDirExists );
