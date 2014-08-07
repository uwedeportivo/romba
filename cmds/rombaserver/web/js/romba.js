
var termFn = function(command, term){
	if (command == 'life is good') {
		term.echo("sure is");
	} else if (command == 'clear') {
		term.clear();	
	} else {
		req = {
			method : "RombaService.Execute",
			params : {
				cmdTxt: command,
				cmdOrigin: "web",
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

var niceBytes = function (bytes) {
    if (bytes == 0) return '0';
    var sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
    i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
    return Math.round(bytes / Math.pow(1024, i), 2) + sizes[i];
};


jQuery(document).ready(function($) {

	$('#progressbarFiles').progressbar();
	$('#progressbarBytes').progressbar();

	var term = $('#terminal').terminal(termFn, {
		login: false,
		greetings: "Welcome to Romba. Type help for commands.",
		onBlur: function() {
			return false;
		}
	});

	var ws = new WebSocket("ws://" + document.location.host + "/progress");

	$('#progress').hide();

	 ws.onmessage = function (e) {
	 	var msg = jQuery.parseJSON(e.data);

	 	if (msg["Running"]) {
		 	$('#progress').show();
		 	if (msg["KnowTotal"]) {
	 		   $('#progressbarBytes').progressbar({ max: msg.TotalBytes });
			   $('#progressbarFiles').progressbar({ max: msg.TotalFiles });
		 	   $('#progressbarFiles').progressbar("value", msg.FilesSoFar);
		       $('#progressbarBytes').progressbar("value", msg.BytesSoFar);
		       $('#progressTextFiles').text("" + msg.FilesSoFar + " of " + msg.TotalFiles);
		 	   $('#progressTextBytes').text("" + niceBytes(msg.BytesSoFar) + " of " + niceBytes(msg.TotalBytes));
		    } else {
		   	   $('#progressbarFiles').progressbar("value", false);
		       $('#progressbarBytes').progressbar("value", false);
		       $('#progressTextFiles').text("" + msg.FilesSoFar);
		 	   $('#progressTextBytes').text("" + niceBytes(msg.BytesSoFar));
		    }
	 	} else {
	 		$('#progress').hide();
	 	}

	 	if (msg["TerminalMessage"] != "") {
	 		term.echo(msg["TerminalMessage"]);
	 	}
	 }
});

