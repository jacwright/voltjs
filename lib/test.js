
var VoltJS = require('./voltjs').VoltJS;
var when = require('./promise').when;

var volt = new VoltJS();
volt.define('Insert', 'string', 'string', 'string');
volt.define('Select', 'string');
volt.define('proc', 'array[string]', 'decimal');

function sayHi(hello, world, language) {
	return function() {
		return volt.call('Insert', hello, world, language).finished(function(results) {
			console.log(results);
		});
	};
}

volt.connect('scooby', 'doo').then(function(results) {
	console.log('connected!');
	
	var english = sayHi('Hello', 'World', 'English');
	var french = sayHi('Bonjour', 'Monde', 'French');
	var spanish = sayHi('Hola', 'Mundo', 'Spanish');
	var danish = sayHi('Hej', 'Verden', 'Danish');
	var italian = sayHi('Hola', 'Mondo', 'Italian');
	
	english().then(french).then(spanish).then(danish).then(italian).then(function() {
		volt.call('Select', 'Spanish').then(function(result) {
			console.log('selected:', result);
			volt.disconnect();
		}, function(err) {
			console.log('error:', err);
			volt.disconnect();
		});
	});
	
}, function(err) {
	console.log('error:', err);
});
