import { json } from 'stream/consumers';
import { WebSocketServer } from 'ws';
const net = require( 'net' );
const fs = require( 'fs' ).promises;
const fs_sync = require( 'fs' );
const { execSync } = require('child_process');

const SRC_BASE = '/usr/local/osdev/source/versionsix/';
const QEMU_QMP_SOCKET = SRC_BASE + 'build_support/logs/qmp_sock';
const QEMU_PID_FILE = SRC_BASE + 'build_support/logs/qemu_pid';
const QEMU_KINFO_FILE = SRC_BASE + 'build_support/logs/kinfo_out';

const wss = new WebSocketServer({ 
	port: 58006,
	clientTracking: true
});

var con;
var qemu_running = false;
var qemu_pid = 0;

function send_to_console( text ) {
	console.log( text );
	fs.appendFile( "/usr/local/osdev/source/versionsix/logs/klog.txt", text + "\n" );
}

wss.on('connection', function connection(ws) {
	con = ws;
	ws.on('error', console.error);

	ws.send( '{"message":"hello", "API":"AVSDevAPI", "version": "1"}' );

	console.log( "sent hello." );

	ws.on('message', function message(data) {
		var expects_reply = false;

		console.log('received: %s', data);

		const message = JSON.parse( data );

		console.log( message );

		switch( message.action ) {
			case 'cat':
				do_cat( message );
				break;
			case 'qmp':
				do_qmp( message );
				break;
			case 'test1':
				do_test1( message );
				break;
			default:
				console.log( "Got unknown action: " + message.action );
		}
	});

	async function do_cat( message ) {
		try {
			const file_data = await fs.readFile( message.file, 'utf8' );

			const response = {
				"message": "response",
				"request_id": message.request_id,
				"action": "cat",
				"file": message.file,
				"data": file_data
			};

			ws.send( JSON.stringify(response) );
		} catch( err ) {
			do_error( err );
		}
	}

	async function do_test1( message ) {
		send_alert( "Hello, alert!" );
	}

	async function do_qmp( message ) {
		var qmp_cmd = JSON.parse( message.command );
		console.log( "qmp cmd: " + qmp_cmd );

		var expect_hello = true;
	
		const qemu_socket = net.createConnection( QEMU_QMP_SOCKET );
		qemu_socket.on( 'connect', () => {
			console.log( "QMP connected" );
			qemu_socket.write( '{ "execute": "qmp_capabilities" }' );
			qemu_socket.write(qmp_cmd);
		});

		qemu_socket.on( 'data', (d) => {
			if( expect_hello ) {
				console.log( "Expected hello, got data: " );
				console.log( d );
				expect_hello = false;
			} else {
				console.log( "Got data from QMP" );
				console.log(d);
			}
		});
	}

	function do_error( err ) {
		ws.send( '{"message" : "error", "data": "' + err + '"}' );
		console.log( "Error: " + err );
	}

	function send_alert( text ) {
		const msg = {
			"message": "alert",
			"text": text
		};

		ws.send( JSON.stringify(msg) );
	}
});

setInterval( () => {
	if( con !== undefined ) {
		var pid_exists = fs_sync.existsSync( QEMU_PID_FILE );
		
		if( pid_exists ) {
			if( !qemu_running ) {
				qemu_running = true;
				qemu_pid = fs_sync.readFileSync( QEMU_PID_FILE, 'utf8' );
				var sys_info_addr = 0;
				var sys_info_addr_cmd = 'nm ' + SRC_BASE + 'build/versionvi.bin' + ' | grep system_information'

				try {
					const cmd_out = execSync( sys_info_addr_cmd, { encoding: 'utf-8' } );
					const sym_info = cmd_out.split(' ');
					sys_info_addr = sym_info[0];
					console.log( "si addr: " + sys_info_addr );
				} catch( err ) {
					console.log( "Error executing sys_info addr read: " + err.message );
				}

				const msg = {
					"message": "qemu_status",
					"status": "running",
					"pid": qemu_pid,
					"sys_info_addr": sys_info_addr
				};

				console.log( "sending status update:" );
				console.log( msg );

				con.send( JSON.stringify(msg) );
			}
		} else {
			if( qemu_running ) {
				qemu_running = false;
				
				const msg = {
					"message": "qemu_status",
					"status": "stopped",
					"pid": qemu_pid
				};

				con.send( JSON.stringify(msg) );

				qemu_pid = 0;
			}
		}
	}
}, 500 );

var thequeue = [];

class QEMUData {
	constructor() {
		this.recv_buffer = "";
		this.waiting_to_emit = [];
		this.socket = undefined;

		this.START = "AVSDEVSTART";
		this.END = "AVSDEVEND";
	}

	add_to_recv( d ) {
		this.recv_buffer = this.recv_buffer + d.toString();
		var newline = this.recv_buffer.indexOf( "\n" );

		if( newline !== -1 ) {
			var out = this.recv_buffer.substring( 0, newline );
			out = out.replaceAll( "\n", "" );

			this.recv_buffer = this.recv_buffer.substring( newline, 0 );

			var input_good = false;
			var j;

			try {
				thequeue.push( out );
					
				//console.log( "Message is: \"" + this.recv_buffer + "\"" );
				j = JSON.parse( out.toString() );

				input_good = true;
			} catch(e) {
				console.log( "⛔ Got bad input: -->" + this.recv_buffer.toString() + "<--\n" );
			}

			if( input_good ) {
				if( j.crash !== undefined ) {
					this.klog_crash_to_console( j );
				} else if( j.cmd !== undefined ) {
					this.do_cmd( j );
				} else {
					this.klog_normal_msg_to_console( j );
				}
			}

			this.recv_buffer = "";
		} else {
			//console.log( "Buffer is: \"" + this.recv_buffer + "\"" );
		}
	}

	klog_crash_to_console( message ) {
		send_to_console( "⛔ Crash address = " + message.crash.address + "  path = " + message.crash.path );

		var linecmd = "addr2line -p --exe=/usr/local/osdev/source/versionsix/build/versionvi.bin " + message.crash.address;

		const cmd_out = execSync( linecmd, { encoding: 'utf-8' } );
		
		send_to_console( "⛔ " + cmd_out );
	}

	klog_normal_msg_to_console( message ) {
		var level = '';

		switch( message.level ) {
			case 'Info':
				level = '🔵';
				break;
			case 'Debug':
				level = '🟢';
				break;
			case 'Error':
				level = '⚠️';
				break;
			case 'Panic':
				level = '🔴';
				break;
		}

		var final_message = level + " " + message.function + "() " + message.line_number + ": " + message.message;

		send_to_console( final_message );
	}

	

	do_cmd( m ) {
		const f = require('fs');

		switch( m.cmd ) {
			case "hello":
				fs.unlink( "/usr/local/osdev/source/versionsix/logs/klog.txt" );

				if( m.build === undefined ) {
					send_to_console( "🔴 Got hello, but missing build" );
					break;
				}
				
				send_to_console( "🌤️ New instance running. Build " + m.build );

				break;
			case "size":
				if( m.file === undefined ) {
					console.log( "🔴 Got size, but missing file." );
					break;
				}
				
				const file_stats = f.statSync( '/usr/local/osdev/source/versionsix/core_apps/finished/' + m.file );
				var file_size = file_stats.size;

				console.log( "🟡 Sending file size of " + m.file + " to client: " + file_size );

				api_socket.write( file_size.toString() + "\n" );

				console.log( "🟡 Done sending" );
				break;
			case "load":
				if( m.file === undefined ) {
					console.log( "🔴 Got load, but missing file." );
					break;
				}
				
				var file_size = 0;
				const file_data = f.readFileSync( '/usr/local/osdev/source/versionsix/core_apps/finished/' + m.file );
								
				var encoded_data = file_data.toString('base64');

				api_socket.write( encoded_data + "\n" );

				console.log( "🟡 Sending file contents of " + m.file + " to client." );
				break;
			default:
				console.log( "🔴 Unknonw Command: " + m.cmd );
		}
	}
};

var api_socket = [];
var data_handler = new QEMUData();

const server = net.createServer((socket) => {
	socket.on( "data", (d) => {
		/* thequeue.push(d);
		console.log( "QEMU Sent: " + d.toString() );		 */

		data_handler.add_to_recv(d);
	});

	socket.on( "ready", () => {
		console.log( "socket ready.\n" )
	});

	api_socket = socket;

	// Do nothing else, this is ingest only
});

server.listen( 58007, "plato.marsdev.io" );