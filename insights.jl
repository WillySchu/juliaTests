module Insights

function harvestInsights(arr::Array{Any,1})
  println("Harvesting...")
  results = []
  if length(arr) < 2
    error("Not enough data to process")
  end

  push!(results, dayByDay(arr))
  if length(arr) < 14
    return results
  end
  push!(results, weekByWeek(arr))
  if length(arr) < 60
    return results
  end

  return results
end

function dayByDay(arr::Array{Any, 1})
  println("Comparing by day...")
  today = aggregate(arr[end:end])
  yesterday = aggregate(arr[end-1:end-1])
  diff = compare(today, yesterday)
  insights = generateInsights(diff, 5)
  return insights
end

function weekByWeek(arr::Array{Any, 1})
  println("Comparing by week...")
  thisWeek = aggregate(arr[end-6:end])
  lastWeek = aggregate(arr[end-13:end-7])
  diff = compare(thisWeek, lastWeek)
  insights = generateInsights(diff, 5)
  return insights
end

function generateInsights(diff::Dict{String, Any}, n::Int64)
  println("Generating...")
  insights = []
  meta = diff["meta"]
  for met in keys(diff)
    if met == "meta"
      continue
    end
    for dim in keys(diff[met])
      insight = Dict{String, Any}()
      insight["startDate"] = meta["startDate"]
      insight["endDate"] = meta["endDate"]
      insight["metrics"] = met
      insight["dimensions"] = dim
      insight["type"] = "type"
      insight["percentChange"] = diff[met][dim]["score"]
      insight["significance"] = diff[met][dim]["significance"]
      push!(insights, insight)
    end
  end
  sort!(insights, by=x->x["significance"])
  return insights[1:n]
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
      diff[met] = Dict{String, Any}()
    end
    println(met)
    for dim in keys(first[met])
      diff[met][dim] = Dict{String, Float64}()
      if haskey(second[met], dim)
        push!(inn, dim)
        if first[met][dim] == 0
          if second[met][dim] == 0
            diff[met][dim]["score"] = 0
          else
            diff[met][dim]["score"] = -1
          end
        else
          diff[met][dim]["score"] = (first[met][dim] - second[met][dim]) / second[met][dim]
        end
        if diff[met][dim]["score"] == Inf
          diff[met][dim]["significance"] = (first[met][dim] + second[met][dim])^2 * 100
        else
          diff[met][dim]["significance"] = (first[met][dim] + second[met][dim])^2 * diff[met][dim]["score"]
        end
      else
        diff[met][dim]["score"] = Inf
        diff[met][dim]["significance"] = first[met][dim]^2 * 100
        push!(out, dim)
      end
    end
  end
  # for met in keys(diff)
  #   for key in keys(diff[met])
  #     println(key)
  #     println(diff[met][key])
  #     println(">>>>>>>>>>>>>>>>>>>>>>")
  #   end
  # end
  meta = Dict{String, Any}()
  meta["startDate"] = second["meta"]["startDate"]
  meta["endDate"] = first["meta"]["endDate"]
  diff["meta"] = meta
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
