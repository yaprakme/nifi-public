	local table = {} 
	
	local members = redis.call('SMEMBERS', KEYS[1])
	if members == nil then
       return nil
    end   
	
	
	for i,line in ipairs(members) do
	   
	  local hashes = redis.call('HGETALL', line)
	  if hashes ~= nil then 
		  for i = 1, 1000000, 2 do
		      if hashes[i] == nil or hashes[i+1] == nil then
			     break
			  end    
			  
			  local key = hashes[i]
			  local value = hashes[i+1]
			 
			  -- calculate sum
			  if table[key ..'.sum'] == nil then
			    table[key ..'.sum'] = value
			  else
			   table[key ..'.sum'] = table[key ..'.sum'] + value
			  end
			  
			  -- calculate count
			  if table[key ..'.count'] == nil then
			    table[key ..'.count'] = 1
			  else
			   table[key ..'.count'] = table[key ..'.count'] + 1
			  end
			   
		  end	 
	   end
	end
	
    local returnArr = {}
    local returnArrI = 1
	for k,v in pairs(table) do
	   returnArr[returnArrI]= (k ..'')
	   returnArrI = returnArrI + 1
	   returnArr[returnArrI]= (v ..'')
	   returnArrI = returnArrI + 1
	end 
	
	return returnArr
	 
	