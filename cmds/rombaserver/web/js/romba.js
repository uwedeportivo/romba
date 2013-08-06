
var terminalObject;

var termFn = function(command, term){
	terminalObject = term;

	if (command == 'life is good') {
		term.echo("sure is");
	} else {
		req = {
			method : "RombaService.Execute",
			params : {
				cmdTxt: command,
			},
		};
		$.jsonrpc(req, {
			success : function(result) {
				term.echo(result.Message);
			},
			error : function(error) {
				term.echo("error: " + error)
			},
		});
	}   
}

jQuery(document).ready(function($) {

	$('#progressbarFiles').progressbar();
	$('#progressbarBytes').progressbar();

	$('#terminal').terminal(termFn, {
		login: false,
		greetings: "Welcome to Romba. Type help for commands.",
		onBlur: function() {
			return false;
		}
	});

	var ws = new WebSocket("ws://localhost:4200/progress");

	$('#progress').hide();

	 ws.onmessage = function (e) {
	 	var msg = jQuery.parseJSON(e.data);

	 	if (msg["Running"]) {
	 		if (msg["Starting"]) {
	 			$('#progress').show();
	 		}
	 		$('#progressbarBytes').progressbar({ max: msg.TotalBytes });
			$('#progressbarFiles').progressbar({ max: msg.TotalFiles });
		 	$('#progressbarFiles').progressbar("value", msg.FilesSoFar);
		 	$('#progressbarBytes').progressbar("value", msg.BytesSoFar);
		 	$('#progressText').text(e.data);
	 	} else {
	 		$('#progress').hide();
	 	}

	 	if (msg["TerminalMessage"] != "") {
	 		terminalObject.echo(msg["Terminalmessage"]);
	 	}
	 }
});

