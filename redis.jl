using Redis

config = include("json.jl")

host = config.redis["host"]
port = config.redis["port"]
port = parse(Int, port)
pass = config.redis["pass"]

conn = RedisConnection(
  host=host,
  port=port,
  password=pass
)

key = get(conn, "key")

println(key)
