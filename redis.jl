using Redis

# config = include("json.jl")
#
# host = config.redis["host"]
# port = config.redis["port"]
# port = parse(Int, port)
# pass = config.redis["pass"]
#
# conn = RedisConnection(
#   host=host,
#   port=port,
#   password=pass
# )

conn = RedisConnection()

key = get(conn, "key")

# println(key)
# println(typeof(key))

while(true)
  key = get(conn, "key")
  if (typeof(key) != Void)
    println(key)
    del(conn, "key")
    break
  end
end
