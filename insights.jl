module Insights

function harvestInsights(arr::Array{Any,1})
  println("Harvesting...")
  results = []
  if length(arr) < 2
    error("Not enough data to process")
  end

  push!(results, dayByDay(arr))
end

function dayByDay(arr::Array{Any, 1})
  println("Comparing by day")
  today = aggregate(arr[end:end])
  yesterday = aggregate(arr[end-1:end-1])
  diff = compare(today, yesterday)

  return Dict()
end

function compare(first::Dict{String, Any}, second::Dict{String, Any})
  println("Comparing...")
  inn = []
  out = []
  diff = Dict{String, Any}()
  for met in keys(first)
    if met == "meta"
      continue
    end

    if !haskey(diff, met)
      diff[met] = Dict{String, Float64}()
    end
    println(met)
    for dim in keys(first[met])
      if haskey(second[met], dim)
        push!(inn, dim)
        if first[met][dim] == 0
          if second[met][dim] == 0
            diff[met][dim] = 0
          else
            diff[met][dim] = -1
          end
        else
          diff[met][dim] = (first[met][dim] - second[met][dim]) / second[met][dim]
        end
      else
        push!(out, dim)
      end
    end
  end
  for met in keys(diff)
    for key in keys(diff[met])
      println(key)
      println(diff[met][key])
      println(">>>>>>>>>>>>>>>>>>>>>>")
    end
  end
  return diff
end

function aggregate(arr::Array{Any, 1})
  agg = Dict{String, Any}()
  meta = Dict{String, Any}()
  agg["meta"] = meta
  dimensions = arr[1]["query"]["dimensions"]
  metrics = arr[1]["query"]["metrics"]
  dateRegex = r".+?(?=T)"
  for met in metrics
    agg[met] = Dict()
  end
  x = 0
  for day in arr
    startDate = Date(match(dateRegex, day["query"]["start-date"]).match)
    endDate = Date(match(dateRegex, day["query"]["end-date"]).match)

    if startDate != endDate
      error("Harvestor received non exploded data")
    end

    if !haskey(meta, "startDate")
      meta["startDate"] = startDate
    end
    if !haskey(meta, "endDate")
      meta["endDate"] = endDate
    end

    if startDate < meta["startDate"]
      meta["startDate"] = startDate
    end

    if endDate > meta["endDate"]
      meta["endDate"] = endDate
    end

    for i = 1:length(day["rows"])
      dimName = ""
      for j = 1:length(day["rows"][1])
        if day["columnHeaders"][j]["columnType"] == "DIMENSION"
          dimName = string(dimName, day["rows"][i][j])
        else
          met = day["columnHeaders"][j]["name"]
          if haskey(agg[met], dimName)
            agg[met][dimName] += parse(Float64, day["rows"][i][j])
          else
            agg[met][dimName] = parse(Float64, day["rows"][i][j])
          end
        end
      end
    end
  end
  return agg
end
end
