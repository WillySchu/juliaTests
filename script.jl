using HttpServer
using WebSockets

wsh = WebSocketHandler() do req,client
  while true
    msg = read(client)
    println(msg)
    write(client, msg)
  end
end

server = Server(wsh)
run(server, 8080)
