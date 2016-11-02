module Config

using JSON

path = "/home/ubuntu/Dropbox/code/local/config/development/node/config.json"

data = JSON.parsefile(path)

redis = data["env"]["will"]["redis"]
println(redis)

export redis

end
