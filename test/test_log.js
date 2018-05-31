'use strict';

var log		= require( '../log.js' );



function test_log()
{
	log.consoleLog( "111111", "222222", { "key1" : "a", "key2" : "b" }, [ "a1", "a2", "a3" ] );
}


test_log();