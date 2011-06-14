
var Parser = require('./parser').Parser;

function Message(buffer) {
	Parser.call(this, buffer);
	if (!buffer) {
		this.writeInt(0);
		this.writeByte(0);
	} else {
		this.readHeader();
	}
}

Message.prototype = Object.create(Parser.prototype);

Message.prototype.readHeader = function() {
	this.length = this.readInt();
	this.protocol = this.readByte();
};

Message.prototype.writeHeader = function() {
	var pos = this.position;
	this.position = 0;
	this.writeInt(this.buffer.length - 4);
	this.writeByte(0);
	this.position = pos;
};

Message.prototype.toBuffer = function() {
	this.writeHeader();
	return new Buffer(this.buffer);
};

// for getting lengths from incoming data
Message.readInt = function(buffer, offset) {
	return Parser.readInt(buffer, offset);
};

exports.Message = Message;