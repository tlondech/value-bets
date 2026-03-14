#!/usr/bin/env python3
"""Local dev server — suppresses Chrome DevTools 404 noise."""
import json
from http.server import SimpleHTTPRequestHandler, HTTPServer

class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/.well-known/appspecific/com.chrome.devtools.json":
            body = json.dumps({}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            super().do_GET()

    def log_message(self, fmt, *args):
        # Suppress the devtools probe from the log entirely
        if "com.chrome.devtools" not in (args[0] if args else ""):
            super().log_message(fmt, *args)

if __name__ == "__main__":
    server = HTTPServer(("", 8000), Handler)
    print("Serving at http://localhost:8000")
    server.serve_forever()
