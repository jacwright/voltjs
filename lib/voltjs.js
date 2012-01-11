
var EventEmitter = require('events').EventEmitter,
	Socket = require('net').Socket,
	crypto = require('crypto'),
	Message = require('./message').Message,
	Deferred = require('./promise').Deferred;


// TODO add general events for error/connect/disconnect
function VoltJS(host, port, callTimeout) {
	EventEmitter.call(this);
	this.host = host || 'localhost';
	this.port = port || 21212;
	this.callTimeout = callTimeout || 60000; // one minute timeout
	this._id = 0;
	this._calls = {};
	this._defs = {};
	this._connection = new Deferred();
	this._overflow = new Buffer(0);
	this.connected = false;
	this.socket = new Socket();
	this.socket.setNoDelay();
	this.onConnect = this.onConnect.bind(this);
	this.onLogin = this.onLogin.bind(this);
	this.onCall = this.onCall.bind(this);
	this.onData = this.onData.bind(this);
	this.onError = this.onError.bind(this);
	this.socket.on('error', this.onError);
//	this.socket.on('data', this.onCall);
	this.socket.on('data', this.onData);
}

VoltJS.prototype = Object.create(EventEmitter.prototype);

VoltJS.prototype.connect = function(username, password, host, port, callback) {
	if (!callback && typeof arguments[arguments.length - 1] === 'function')
		callback = arguments[arguments.length - 1];

	if (!port || typeof host === 'function') port = this.port;
	if (!host || typeof host === 'function') host = this.host;
	var self = this;
	this._connection.reset();
	this.socket.connect(port, host);
	
	this.socket.on('connect', this.onConnect);
	this.socket.on('error', this.onConnect);
	this.socket.on('disconnect', this.onDisconnect);
	
	var promise = this._connection.promise;
	if (username) {
		promise = promise.then(function() {
			return self.login(username, password);
		});
	}
	
	queueCallback(promise, callback);
	
	return promise;
};

VoltJS.prototype.disconnect = function() {
	this.socket.end();
};

VoltJS.prototype.login = function(username, password, callback) {
	var deferred = this._calls.login = new Deferred();
	var service = 'database';
	var sha1 = crypto.createHash('sha1');
	sha1.update(password);
	password = new Buffer(sha1.digest('binary'), 'binary');
	
	this.removeListener('data', this.onCall);
	this.on('data', this.onLogin);
	
	var message = new Message();
	message.writeString(service);
	message.writeString(username);
	message.writeBinary(password);
	this.send(message);
	
	queueCallback(deferred.promise, callback);
	
	return deferred.promise;
};

VoltJS.prototype.define = function(procedure) {
	this._defs[procedure] = Array.prototype.slice.call(arguments, 1);
};


VoltJS.prototype.call = function(procedure, callback) {
	var params = Array.prototype.slice.call(arguments, 1);
	if (params.length && !this._defs.hasOwnProperty(procedure))
		throw new Error('Param types must be defined previously using voltJS.define(procedure, param1Type, param2Type);');
	
	var promise = (this.connected)
			? this._callProc(procedure, params)
			: this._connection.then(this._callProc.bind(this, procedure, params));
	
	queueCallback(promise, callback);
	return promise;
};

VoltJS.prototype._callProc = function(procedure, params) {
	var deferred = new Deferred();
	var clientData = this.getCallId();
	var types = this._defs[procedure];
	this._calls[clientData] = deferred;
	deferred.timeout(this.callTimeout);
	
	var message = new Message();
	message.writeString(procedure);
	message.writeBinary(new Buffer(clientData));
	message.writeParameterSet(types, params);
	
	this.send(message);
	return deferred.promise.failed(this.cleanup.bind(this, clientData));
};


VoltJS.prototype.onConnect = function(exception) {
	this.socket.removeListener('connect', this.onConnect);
	this.socket.removeListener('error', this.onConnect);
	
	this.connected = true;
	if (exception) this._connection.fail(exception, this);
	else this._connection.fulfill(this);
};


VoltJS.prototype.onDisconnect = function() {
//	this.socket.addListener('connect', this.onConnect);
//	this.socket.addListener('error', this.onConnect);
	
	this.connected = false;
//	if (exception) this._connection.fail(exception, this);
//	else this._connection.fulfill(this);
};


// not needed, always came back as whole packets
VoltJS.prototype.onData = function(buffer) {
//	console.log('more data, incoming:', buffer.length, 'existing:', this._overflow.length);
	
	var overflow = this._overflow;
	var data = new Buffer(overflow.length + buffer.length);
	var length;
		
	overflow.copy(data, 0);
	buffer.copy(data, overflow.length, 0);
//	console.log('Total data length:', data.length, 'Next Message Length:', Message.readInt(data) + 4);
	
	while (data.length > 4 && data.length >= (length = Message.readInt(data) + 4) ) {
//		console.log('Reading Data Length:', length);
		var lastMsg = msg;
		var msg = data.slice(0, length);
		data = data.slice(length);
//		if (length > 54) console.log(length, msg.length, msg);
		this.emit('data', msg); // TODO move this into a worker probably
//		console.log('Data Size:', data.length);
	}
	this._overflow = data;
//	console.log('Leftovers:', data.length);
};


VoltJS.prototype.onLogin = function(buffer) {
	var deferred = this._calls.login;
	delete this._calls.login;
	
	this.removeListener('data', this.onLogin);
	this.on('data', this.onCall);
	
	var message = new Message(buffer);
	var code = message.readByte();
	
	if (code === 0) {
		var serverId = message.readInt();
		var connectionId = message.readLong();
		var clusterStartTimestamp = new Date(parseInt(message.readLong().toString())); // not microseonds, milliseconds
		var leaderIP = message.readByte() + '.' + message.readByte() + '.' + message.readByte() + '.' + message.readByte();
		var build = message.readString();
		
		var results = {
			serverId: serverId,
			connectionId: connectionId,
			clusterStartTimestamp: clusterStartTimestamp,
			leaderIP: leaderIP,
			build: build
		};
		
		deferred.fulfill(results);
	} else {
		deferred.fail(new Error(LOGIN_ERRORS[code]));
	}
};

VoltJS.prototype.onCall = function(buffer) {
	var message = new Message(buffer);
	
	var clientData = message.readBinary(8).toString();
	var fieldsPresent = message.readByte(); // bitfield, use PRESENT values to check
	var status = message.readByte();
	var statusString = STATUS_CODE_STRINGS[status];
	if (fieldsPresent & PRESENT.STATUS) {
		statusString = message.readString();
	}
	var appStatus = message.readByte();
	var appStatusString = '';
	if (fieldsPresent & PRESENT.APP_STATUS) {
		appStatusString = message.readString();
	}
	var exception;
	var exceptionLength = message.readInt();
	if (fieldsPresent & PRESENT.EXCEPTION) {
		exception = message.readException(1); // seems size doesn't matter, always 1
	} else { // don't parse the rest if there was an exception. Bad material there.
		
		var resultCount = message.readShort();
		if (resultCount != 0) resultCount = 1;
		var results = new Array(resultCount);
		for (var i = 0; i < resultCount; i++) {
			results[i] = message.readVoltTable();
		}
	}
	
	// may have timed out, don't assume it is there.
	if (clientData in this._calls) {
		var deferred = this._calls[clientData];
//		if (exception) deferred.fail(exception);
		if (status != STATUS_CODES.SUCCESS) deferred.fail(new Error(statusString));
		else deferred.fulfill(results.length == 1 ? results[0] : results); // if just one result table return only that.
		delete this._calls[clientData];
	}
};

VoltJS.prototype.onError = function() {
	// TODO handle connection errors and connection close
};

VoltJS.prototype.getCallId = function() {
	var id = String(this._id < 0xFFFFFFFF ? this._id++ : this._id = 0);
	return zeros(8 - id.length).join('') + id;
};

VoltJS.prototype.cleanup = function(clientData, err) {
	delete this._calls[clientData];
	return err;
};


VoltJS.prototype.send = function(message) {
	var buffer = message.toBuffer();
	this.socket.write(buffer);
};


var PRESENT = {
	STATUS: 0x20,
	EXCEPTION: 0x40,
	APP_STATUS: 0x80
};

var STATUS_CODES = {
	SUCCESS            :  1,
	USER_ABORT         : -1,
	GRACEFUL_FAILURE   : -2,
	UNEXPECTED_FAILURE : -3,
	CONNECTION_LOST    : -4
};

var STATUS_CODE_STRINGS = {
	1: 'SUCCESS',
	'-1': 'USER_ABORT',
	'-2': 'GRACEFUL_FAILURE',
	'-3': 'UNEXPECTED_FAILURE',
	'-4': 'CONNECTION_LOST'
};

var LOGIN_ERRORS = {
	1: 'Too many connections',
	2: 'Authentication failed, client took too long to transmit credentials',
	3: 'Corrupt or invalid login message'
};


function zeros(num) {
	var arr = new Array(num);
	for (var i = 0; i < num; i++) arr[i] = 0;
	return arr;
}

function queueCallback(promise, callback) {
	if (!callback) return;
	promise.then(function(results) {
		callback(null, results);
	}, function(err) {
		callback(err);
	});
}

exports.VoltJS = VoltJS;